<?php

declare(strict_types=1);

require __DIR__ . '/../../vendor/autoload.php';

// Import datasets by NodeJs script
passthru(sprintf('nodejs %s/fixtures/import/importDatasets.js', __DIR__), $exitCode);
if ($exitCode !== 0) {
    exit($exitCode);
}
