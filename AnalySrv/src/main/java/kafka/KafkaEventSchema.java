/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * The serialization schema for the {@link KafkaEvent} type. This class defines how to transform a
 * Kafka record's bytes to a {@link KafkaEvent}, and vice-versa.
 */

/**
 * 反序列化shema:
 * 		Flink Kafka Consumer 需要知道如何将来自Kafka的二进制数据转换为Java/Scala对象。DeserializationSchema接口允许程序员指定这个序列化的实现。
 * 		我们通常会实现 AbstractDeserializationSchema，它可以描述被序列化的Java/Scala类型到Flink的类型(TypeInformation)的映射。如果用户的代码
 * 实现了DeserializationSchema，那么就需要自己实现getProducedType(...) 方法。
 */
public class KafkaEventSchema implements DeserializationSchema<KafkaEvent>, SerializationSchema<KafkaEvent> {

	private static final long serialVersionUID = 6154188370181669758L;

	@Override
	public byte[] serialize(KafkaEvent event) {
		return event.toString().getBytes();
	}

	@Override
	public KafkaEvent deserialize(byte[] message) throws IOException {
		return KafkaEvent.fromString(new String(message));
	}

	@Override
	public boolean isEndOfStream(KafkaEvent nextElement) {
		return false;
	}

	@Override
	public TypeInformation<KafkaEvent> getProducedType() {
		return TypeInformation.of(KafkaEvent.class);
	}
}

