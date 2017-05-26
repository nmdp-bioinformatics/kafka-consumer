package org.nmdp.kafkaconsumer.consumer;

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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.nmdp.kafkaconsumer.config.KafkaConsumerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public class KafkaMessageConsumer extends Thread implements Closeable {

    private static final String NEWLINE = System.getProperty("line.separator");
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageConsumer.class);

    private final KafkaConsumer<byte[], byte[]> consumer;

    public KafkaMessageConsumer() {
        this.consumer = instantiateKafkaConsumer();
    }

    private static KafkaConsumer<byte[], byte[]> instantiateKafkaConsumer() {
        return new KafkaConsumer<>(KafkaConsumerProperties.getConfig(),
            new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
}
