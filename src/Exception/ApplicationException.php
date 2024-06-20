<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Exception;

use Exception;
use Keboola\CommonExceptions\ApplicationExceptionInterface;

class ApplicationException extends Exception implements ApplicationExceptionInterface
{

}
