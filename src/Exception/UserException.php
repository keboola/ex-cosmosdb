<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Exception;

use Exception;
use Keboola\CommonExceptions\UserExceptionInterface;

class UserException extends Exception implements UserExceptionInterface
{

}
