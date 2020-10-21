<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Tests;

use CosmosDbExtractor\Exception\ProcessException;
use PHPUnit\Framework\Assert;

class ProcessFactoryTest extends AbstractTestCase
{
    public function testSuccessfulProcess(): void
    {
        $this->createScriptProcess('stdoutAndStderr.js');
        $this->loop->run();

        Assert::assertTrue($this->logger->hasInfoThatContains('stdout1'));
        Assert::assertTrue($this->logger->hasInfoThatContains('stdout2'));
        Assert::assertTrue($this->logger->hasInfoThatContains('stdout3'));
        Assert::assertTrue($this->logger->hasInfoThatContains('stdout4'));
        Assert::assertTrue($this->logger->hasWarningThatContains('stderr1'));
        Assert::assertTrue($this->logger->hasWarningThatContains('stderr2'));
        Assert::assertTrue($this->logger->hasDebugThatMatches('~Process ".*" completed successfully.~'));
    }

    public function testFailedProcess(): void
    {
        $this->createScriptProcess('exitCode.js');

        try {
            $this->loop->run();
            Assert::fail('Exception expected.');
        } catch (ProcessException $e) {
            Assert::assertSame(ProcessException::class, get_class($e));
            Assert::assertStringMatchesFormat('Process "%a/exitCode.js" exited with code "123".', $e->getMessage());
        }

        Assert::assertTrue($this->logger->hasWarningThatContains('stderr1'));
        Assert::assertTrue($this->logger->hasWarningThatContains('stderr2'));
        Assert::assertFalse($this->logger->hasDebugThatMatches('~Process ".*" completed successfully.~'));
    }
}
