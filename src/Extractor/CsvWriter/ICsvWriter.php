<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Extractor\CsvWriter;

interface ICsvWriter
{
    /**
     * Write single decoded JSON document to CSV file
     */
    public function writeItem(object $item): void;

    /**
     * Called when all items are written
     */
    public function finalize(): void;

    /**
     * Write last state for incremental fetching
     */
    public function writeLastState(array $inputState): void;
}
