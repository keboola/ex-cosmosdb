<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Configuration;

use CosmosDbExtractor\Configuration\Node\DbNode;
use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ActionConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->ignoreExtraKeys(true)
            ->children()
                ->append(new DbNode())
        ;
        // @formatter:on
        return $parametersNode;
    }
}
