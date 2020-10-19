<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor;

use React\Stream\ReadableStreamInterface;

/**
 * JsonDecoder decodes a stream of JSON documents separated by the DELIMITER.
 * This is a memory-efficient way to process a large set of the JSON documents.
 * Eg. {...json1...}DELIMITER{...json2...}DELIMITER{...json3...}
 */
class JsonDecoder
{
    public const DELIMITER = "\n---\n";

    private string $buffer = '';

    public function processChunk(string $chunk): iterable
    {
        $this->buffer .= $chunk;
        return $this->parse();
    }

    public function processStream(ReadableStreamInterface $stream, callable $worker): void
    {
        // New data is processed when it arrives
        $stream->on('data', fn(string $chunk) => $worker($this->processChunk($chunk)));
        // At the end, the rest of the buffer is parsed
        $stream->on('end', fn() => $worker($this->processChunk(self::DELIMITER)));
        // Throw exception when it occurs
        $stream->on('error', function (\Throwable $e): void {
            throw $e;
        });
    }

    protected function parse(): iterable
    {
        // Keep parsing while the delimiter has been found
        while (($delimiter = strpos($this->buffer, self::DELIMITER)) !== false) {
            // Split buffer by delimiter
            $json = (string) substr($this->buffer, 0, $delimiter);
            $this->buffer = (string) substr($this->buffer, $delimiter + strlen(self::DELIMITER));

            // Decode JSON document, throw the JsonException on the error
            yield json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        }
    }
}
