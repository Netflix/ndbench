package com.netflix.ndbench.plugin.janusgraph.configs.cql;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.ndbench.api.plugin.common.NdBenchConstants;

/**
 * Specific configs for JanusGraph's CQL backend
 * 
 * @author pencal
 */

@Configuration(prefix = NdBenchConstants.PROP_NAMESPACE + ".janusgraph.storage.cql")
public interface ICQLConfig {

	@DefaultValue("ndbench_cql")
	String getKeyspace();

	@DefaultValue("na")
	String getClusterName();
}
