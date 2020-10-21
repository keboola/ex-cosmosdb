<?php

declare(strict_types=1);

namespace CosmosDbExtractor\Configuration;

use CosmosDbExtractor\Configuration\Node\DbNode;
use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigDefinition extends BaseConfigDefinition
{
    public const DEFAULT_MAX_TRIES = 5;

    public const MODE_RAW = 'raw';
    public const MODE_MAPPING = 'mapping';

    private const QUERY_INCOMPATIBLE_NODES = ['select', 'sort', 'limit', 'incrementalFetchingKey'];

    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->ignoreExtraKeys(true)
            ->children()
                ->append(new DbNode())
                ->integerNode('id')->isRequired()->min(0)->end()
                ->scalarNode('name')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('containerId')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('output')->isRequired()->cannotBeEmpty()->end()
                ->integerNode('retries')->min(0)->defaultValue(self::DEFAULT_MAX_TRIES)->end()
                // Generated query
                ->scalarNode('select')->defaultNull()->cannotBeEmpty()->end()
                ->scalarNode('sort')->defaultNull()->cannotBeEmpty()->end()
                ->integerNode('limit')->defaultNull()->end()
                // Custom query
                ->scalarNode('query')->defaultNull()->cannotBeEmpty()->end()
                // Mapping
                ->enumNode('mode')
                    ->values([self::MODE_MAPPING, self::MODE_RAW])
                    ->defaultValue(self::MODE_MAPPING)
                ->end()
                ->variableNode('mapping')->end()
                // Incremental loading
                ->booleanNode('incremental')->defaultValue(false)->end()
                // Incremental fetching
                ->scalarNode('incrementalFetchingKey')->defaultNull()->end()

        ;
        // @formatter:on

        // Validation
        $parametersNode->validate()->always(function ($v) {
            // Custom or generated query can be used, not both.
            foreach (self::QUERY_INCOMPATIBLE_NODES as $node) {
                if (isset($v['query']) && isset($v[$node])) {
                    throw new InvalidConfigurationException(sprintf(
                        'Invalid configuration, "query" cannot be configured together with "%s".',
                        $node
                    ));
                }
            }

            // Validate mode
            switch ($v['mode']) {
                case self::MODE_RAW:
                    if (isset($v['mapping'])) {
                        throw new InvalidConfigurationException(
                            'Invalid configuration, "mapping" is configured, but mode is set to "raw".'
                        );
                    }
                    break;

                case self::MODE_MAPPING:
                    if (!isset($v['mapping'])) {
                        throw new InvalidConfigurationException(
                            'Invalid configuration, missing "mapping" key, mode is set to "mapping".'
                        );
                    }
                    break;

                default:
                    throw new InvalidConfigurationException(sprintf('Unexpected mode "%s".', $v['mode']));
            }

            return $v;
        });

        return $parametersNode;
    }
}
