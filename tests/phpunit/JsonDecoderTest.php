<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Tests;

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
        $this->jsonDecoder->processStream(
            $process->getJsonStream(),
            function (object $document) use (&$parsedDocuments): void {
                // Convert object to array for asserts
                $parsedDocuments[] = (array) $document;
            }
        );

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
        $this->jsonDecoder->processStream($process->getJsonStream(), function (): void {
        });

        $this->expectException(JsonException::class);
        $this->loop->run();
    }

    public function getValidScripts(): array
    {
        return [
            'emptyOutput.js' => [
                'json-decoder/emptyOutput.js',
                [],
            ],
            'oneDocument1.js' => [
                'json-decoder/oneDocument1.js',
                [
                    ['a' => 'b', 'c' => 'd'],
                ],
            ],
            'oneDocument2.js' => [
                'json-decoder/oneDocument2.js',
                [
                    ['a' => 'b', 'c' => 'd'],
                ],
            ],
            'manyDocuments.js' => [
                'json-decoder/manyDocuments.js',
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
            'invalidJson1.js' => ['json-decoder/invalidJson1.js'],
            'invalidJson2.js' => ['json-decoder/invalidJson2.js'],
        ];
    }
}
