<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor;

use Generator;
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

    public function processChunk(string $chunk): array
    {
        $this->buffer .= $chunk;
        return iterator_to_array($this->parse());
    }

    public function processStream(ReadableStreamInterface $stream, callable $worker): void
    {
        // New data is processed when it arrives
        $stream->on('data', function (string $chunk) use ($worker): void {
            foreach ($this->processChunk($chunk) as &$document) {
                $worker($document);
            }
        });

        // At the end, the rest of the buffer is parsed
        $stream->on('end', function () use ($worker): void {
            foreach ($this->processChunk(self::DELIMITER) as &$document) {
                $worker($document);
            }
        });

        // Throw exception when it occurs
        $stream->on('error', function (\Throwable $e): void {
            throw $e;
        });
    }

    protected function parse(): Generator
    {
        // Keep parsing while the delimiter has been found
        while (($delimiter = strpos($this->buffer, self::DELIMITER)) !== false) {
            // Split buffer by delimiter
            $json = (string) substr($this->buffer, 0, $delimiter);
            $this->buffer = (string) substr($this->buffer, $delimiter + strlen(self::DELIMITER));

            // Decode JSON document, throw the JsonException on the error
            if (trim($json) !== '') {
                yield json_decode($json, false, 512, JSON_THROW_ON_ERROR);
            }
        }
    }
}
