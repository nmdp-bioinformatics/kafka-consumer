package org.nmdp.kafkaconsumer.config;

/**
 * Created by Andrew S. Brown, Ph.D., <andrew@nmdp.org>, on 5/26/17.
 * <p>
 * kafka-consumer
 * Copyright (c) 2012-2017 National Marrow Donor Program (NMDP)
 * <p>
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; either version 3 of the License, or (at
 * your option) any later version.
 * <p>
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; with out even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library;  if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA.
 * <p>
 * > http://www.fsf.org/licensing/licenses/lgpl.html
 * > http://www.opensource.org/licenses/lgpl-license.php
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.HashMap;

public class KafkaConsumerProperties {
    private Yaml yaml = new Yaml();
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerProperties.class);

    private final BaseProperties baseProperties;

    public KafkaConsumerProperties() throws Exception {
        this.baseProperties = buildBasePropertiesByConfig();
    }

    private BaseProperties buildBasePropertiesByConfig() throws Exception {
        try {
            URL url = new URL("file:./src/main/resources/consumer-configuration.yaml");

            try (InputStream inputStream = url.openStream()) {
                Configuration config = yaml.loadAs(inputStream, Configuration.class);
                BaseProperties.Builder builder = BaseProperties.builder();
                return builder.build(config);
            }
        }catch (Exception ex) {
            LOG.error("Error reading config file.", ex);
            throw ex;
        }
    }

    public static Map<String, Object> getConfig() {
        return new HashMap<>();
    }

    public BaseProperties getBaseProperties() {
        return baseProperties;
    }
}
