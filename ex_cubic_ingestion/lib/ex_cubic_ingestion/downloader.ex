defmodule ExCubicIngestion.Downloader do
  @moduledoc """
  Stream wrapper around HTTPoison.get!(...) that will download at least
  @min_stream_chunk_size of data before sending to stream.

  Modified from source: https://elixirforum.com/t/how-to-stream-file-from-aws-to-client-through-elixir-backend/20693/15?u=bfolkens
  """

  # minimum required for multipart upload to S3
  @min_stream_chunk_size 5 * 1024 * 1024

  @doc """
  Main function of module. Allows for creating a stream from an HTTPoison get!
  """
  @spec stream!(String.t(), module()) :: Enumerable.t()
  def stream!(url, lib_httpoison \\ HTTPoison) do
    Stream.resource(
      # get async with httpoison to initiate stream
      fn ->
        %{
          ref: lib_httpoison.get!(url, %{}, stream_to: self(), async: :once),
          stream_chunk: nil,
          received_chunks_size: 0,
          content_length: 0
        }
      end,
      # construct stream
      fn acc ->
        case receive_response(acc.ref) do
          # returning the chunk to the stream
          {:ok, {:chunk, response_chunk}} ->
            process_chunk(response_chunk, acc, lib_httpoison)

          # extract content length from header, so we can make a determination if
          # we have received all data
          {:ok, {:headers, headers}} ->
            process_headers(headers, acc, lib_httpoison)

          # for all other messages ignore by not sending anything to the stream
          {:ok, msg} ->
            process_status(msg, acc, lib_httpoison)

          {:error, error} ->
            raise("Error during download: #{inspect(error)}")

          :done ->
            {:halt, acc.ref}
        end
      end,
      # lastly, close out request
      fn ref ->
        :hackney.stop_async(ref)
      end
    )
  end

  defp receive_response(ref) do
    id = ref.id

    receive do
      %HTTPoison.AsyncStatus{code: code, id: ^id} when code >= 200 and code < 300 ->
        {:ok, {:status_code, code}}

      %HTTPoison.AsyncStatus{code: code, id: ^id} ->
        {:error, {:status_code, code}}

      %HTTPoison.AsyncHeaders{headers: headers, id: ^id} ->
        {:ok, {:headers, headers}}

      %HTTPoison.AsyncChunk{chunk: chunk, id: ^id} ->
        {:ok, {:chunk, chunk}}

      %HTTPoison.AsyncEnd{id: ^id} ->
        :done
    end
  end

  defp process_chunk(response_chunk, acc, lib_httpoison) do
    # initialize stream chunk if nil
    updated_stream_chunk =
      if is_nil(acc.stream_chunk) do
        response_chunk
      else
        acc.stream_chunk <> response_chunk
      end

    # update how much data we have received so far
    updated_received_chunks_size = acc.received_chunks_size + byte_size(response_chunk)

    # send signal to continue download
    lib_httpoison.stream_next(acc.ref)

    cond do
      # if we are over the minimum required for us to send chunk to stream,
      # send it to stream
      byte_size(updated_stream_chunk) >= @min_stream_chunk_size ->
        {
          [updated_stream_chunk],
          %{acc | stream_chunk: nil, received_chunks_size: updated_received_chunks_size}
        }

      # if we have received all data, send what's left to the stream
      updated_received_chunks_size == acc.content_length ->
        {
          [updated_stream_chunk],
          %{
            acc
            | stream_chunk: updated_stream_chunk,
              received_chunks_size: updated_received_chunks_size
          }
        }

      # for everything else, keep building up the chunk
      true ->
        {
          [],
          %{
            acc
            | stream_chunk: updated_stream_chunk,
              received_chunks_size: updated_received_chunks_size
          }
        }
    end
  end

  defp process_headers(headers, acc, lib_httpoison) do
    # look through headers to get content length
    content_length_from_header =
      Enum.find_value(headers, fn {name, val} ->
        if name == "Content-Length", do: String.to_integer(val)
      end)

    # send signal to continue download
    lib_httpoison.stream_next(acc.ref)

    {[], %{acc | content_length: content_length_from_header || 0}}
  end

  defp process_status(_msg, acc, lib_httpoison) do
    lib_httpoison.stream_next(acc.ref)

    {[], acc}
  end
end
