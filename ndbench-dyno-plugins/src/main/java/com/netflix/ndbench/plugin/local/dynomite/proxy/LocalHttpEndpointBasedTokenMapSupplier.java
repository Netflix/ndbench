package com.netflix.ndbench.plugin.local.dynomite.proxy;

import com.netflix.dyno.connectionpool.impl.lb.HttpEndpointBasedTokenMapSupplier;

public class LocalHttpEndpointBasedTokenMapSupplier extends HttpEndpointBasedTokenMapSupplier{

	public LocalHttpEndpointBasedTokenMapSupplier(int port) {
		super("http://{hostname}:8081/REST/v1/admin/cluster_describe",port);
	}
	
}
