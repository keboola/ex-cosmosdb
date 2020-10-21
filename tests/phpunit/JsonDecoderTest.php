<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Tests;

use CosmosDbExtractor\Extractor\ProcessFactory;
use JsonException;
use CosmosDbExtractor\Extractor\JsonDecoder;
use PHPUnit\Framework\Assert;

class JsonDecoderTest extends AbstractTestCase
{
    private JsonDecoder $jsonDecoder;

    protected function setUp(): void
    {
        parent::setUp();
        $this->jsonDecoder = new JsonDecoder();
    }

    /**
     * @dataProvider getValidScripts
     */
    public function testValidJson(string $script, array $expectedDocuments): void
    {
        $parsedDocuments = [];
        $process = $this->createScriptProcess($script);
        $jsonStream = $process->pipes[ProcessFactory::JSON_STREAM_FD];
        $this->jsonDecoder->processStream($jsonStream, function (array &$document) use (&$parsedDocuments): void {
            $parsedDocuments[] = $document;
        });

        $this->loop->run();
        Assert::assertTrue($this->logger->hasDebugThatMatches('~Process ".*" completed successfully.~'));
        Assert::assertSame($expectedDocuments, $parsedDocuments);
    }

    /**
     * @dataProvider getInvalidScripts
     */
    public function testInvalidJson(string $script): void
    {
        $process = $this->createScriptProcess($script);
        $jsonStream = $process->pipes[ProcessFactory::JSON_STREAM_FD];
        $this->jsonDecoder->processStream($jsonStream, function (): void {
        });

        $this->expectException(JsonException::class);
        $this->loop->run();
    }

    public function getValidScripts(): array
    {
        return [
            'emptyOutput.js' => [
                'emptyOutput.js',
                [],
            ],
            'oneDocument1.js' => [
                'oneDocument1.js',
                [
                    ['a' => 'b', 'c' => 'd'],
                ],
            ],
            'oneDocument2.js' => [
                'oneDocument2.js',
                [
                    ['a' => 'b', 'c' => 'd'],
                ],
            ],
            'manyDocuments.js' => [
                'manyDocuments.js',
                [
                    ['a' => '1', 'c' => 'd'],
                    ['a' => '2', 'c' => 'd'],
                    ['a' => '3', 'c' => 'd'],
                    ['a' => '4', 'c' => 'd'],
                ],
            ],
        ];
    }

    public function getInvalidScripts(): array
    {
        return [
            'invalidJson1.js' => ['invalidJson1.js'],
            'invalidJson2.js' => ['invalidJson2.js'],
        ];
    }
}
