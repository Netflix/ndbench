/*
 *  Copyright 2021 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.netflix.ndbench.plugin.es;

import com.google.inject.ImplementedBy;
import org.apache.http.StatusLine;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;

@ImplementedBy(GenericEsRestClient.class)
public interface EsRestClient extends Closeable {
    void init(List<URI> hosts, EsConfig config);

    StatusLine writeSingleDocument(String index, String docType, String id, String document) throws IOException;

    StatusLine readSingleDocument(String index, String docType, String id) throws IOException;

    StatusLine writeDocumentsBulk(String bulkPayload) throws IOException;
}
