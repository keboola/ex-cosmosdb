<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Tests;

use JsonException;
use CosmosDbExtractor\Extractor\JsonDecoder;
use CosmosDbExtractor\Extractor\ProcessFactory;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\Test\TestLogger;
use React\EventLoop\Factory;
use React\EventLoop\LoopInterface;

class JsonDecoderTest extends TestCase
{
    private JsonDecoder $jsonDecoder;

    private LoggerInterface $logger;

    private LoopInterface $loop;

    private ProcessFactory $processFactory;

    protected function setUp(): void
    {
        parent::setUp();
        $this->jsonDecoder = new JsonDecoder();
        $this->logger = new TestLogger();
        $this->loop = Factory::create();
        $this->processFactory = new ProcessFactory($this->logger, $this->loop);
    }

    /**
     * @dataProvider getValidScripts
     */
    public function testValidJson(string $script, array $expectedDocuments): void
    {
        $parsedDocuments = [];
        $process = $this->processFactory->create(sprintf('node %s/fixtures/%s', __DIR__, $script));
        $this->jsonDecoder->processStream($process->stdout, function (array &$document) use (&$parsedDocuments): void {
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
        $process = $this->processFactory->create(sprintf('node %s/fixtures/%s', __DIR__, $script));
        $this->jsonDecoder->processStream($process->stdout, function (): void {
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
