/*
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.ndbench.core.util;

/**
 * @author vchella
 */
public enum LoadPattern {
    RANDOM("random"),
    SLIDING_WINDOW("sliding_window"),
    SLIDING_WINDOW_FLIP("sliding_window_flip"),
    ZIPFIAN("zipfian");
    private String text;

    LoadPattern(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }

    public static LoadPattern fromString(String text) {
        if (text != null) {
            for (LoadPattern b : LoadPattern.values()) {
                if (text.equalsIgnoreCase(b.text)) {
                    return b;
                }
            }
        }
        return null;
    }
}
