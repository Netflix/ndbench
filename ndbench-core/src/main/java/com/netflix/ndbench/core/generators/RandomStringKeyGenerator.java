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
package com.netflix.ndbench.core.generators;

import java.util.Random;


/**
 * @author vchella
 */
public class RandomStringKeyGenerator extends StringKeyGenerator {
    private final Random kRandom = new Random();

    public RandomStringKeyGenerator(boolean preLoadKeys, int numKeys) {
        super(numKeys, preLoadKeys);
    }

    @Override
    public String getNextKey() {
        int randomKeyIndex = kRandom.nextInt(numKeys);
        if (isPreLoadKeys()) {
            return keys.get(randomKeyIndex);
        } else {
            return "T" + randomKeyIndex;
        }
    }
}
