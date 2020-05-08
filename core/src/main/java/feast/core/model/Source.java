/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.core.model;

import com.google.protobuf.InvalidProtocolBufferException;
import feast.proto.core.SourceProto;
import feast.proto.core.SourceProto.KafkaSourceConfig;
import feast.proto.core.SourceProto.Source.Builder;
import feast.proto.core.SourceProto.SourceType;
import io.grpc.Status;
import java.util.Arrays;
import java.util.Objects;
import javax.persistence.*;
import javax.persistence.Entity;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "sources")
public class Source {

  /** Source Id */
  @Id @GeneratedValue private long id;

  /** Type of the source */
  @Enumerated(EnumType.STRING)
  @Column(name = "type", nullable = false)
  private SourceType type;

  /** Configuration object specific to each source type */
  @Column(name = "config", nullable = false)
  @Lob
  private byte[] config;

  @Column(name = "is_default")
  private boolean isDefault;

  public Source() {
    super();
  }

  /**
   * Construct a source facade object from a given proto object.
   *
   * @param sourceSpec SourceProto.Source object
   * @param isDefault Whether to return the default source object if the source was not defined by
   *     the user
   * @return Source facade object
   */
  public static Source fromProto(SourceProto.Source sourceSpec, boolean isDefault) {
    if (sourceSpec.equals(SourceProto.Source.getDefaultInstance())) {
      Source source = new Source();
      source.setDefault(true);
      return source;
    }

    Source source = new Source();
    source.setType(sourceSpec.getType());

    switch (sourceSpec.getType()) {
      case KAFKA:
        if (sourceSpec.getKafkaSourceConfig().getBootstrapServers().isEmpty()
            || sourceSpec.getKafkaSourceConfig().getTopic().isEmpty()) {
          throw Status.INVALID_ARGUMENT
              .withDescription(
                  "Unsupported source options. Kafka source requires bootstrap servers and topic to be specified.")
              .asRuntimeException();
        }
        source.setConfig(sourceSpec.getKafkaSourceConfig().toByteArray());
        break;
      case UNRECOGNIZED:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unsupported source type. Only [KAFKA] is supported.")
            .asRuntimeException();
    }

    source.setDefault(isDefault);
    return source;
  }

  /**
   * Construct a source facade object from a given proto object.
   *
   * @param sourceSpec SourceProto.Source object
   * @return Source facade object
   */
  public static Source fromProto(SourceProto.Source sourceSpec) {
    return fromProto(sourceSpec, false);
  }

  /**
   * Convert this object to its equivalent proto object.
   *
   * @return SourceProto.Source
   */
  public SourceProto.Source toProto() {
    Builder builder = SourceProto.Source.newBuilder().setType(this.getType());

    byte[] config = this.getConfig();

    switch (this.getType()) {
      case KAFKA:
        KafkaSourceConfig kafkaSourceConfig = null;
        try {
          kafkaSourceConfig = KafkaSourceConfig.parseFrom(config);
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException(
              String.format(
                  "Unable to convert source to proto for source configuration: %s",
                  this.getConfig().toString()),
              e);
        }
        return builder.setKafkaSourceConfig(kafkaSourceConfig).build();
      case INVALID:
      case UNRECOGNIZED:
      default:
        throw new RuntimeException(
            String.format(
                "Unable to convert source to proto for source configuration: %s",
                this.getConfig().toString()));
    }
  }

  /**
   * Override equality for sources. isDefault is always compared first; if both sources are using
   * the default feature source, they will be equal. If not they will be compared based on their
   * type-specific options.
   *
   * @param other other Source
   * @return boolean equal
   */
  public boolean equalTo(Source other) {
    if (other.isDefault() && this.isDefault()
        || (this.getType() == null && other.getType() == null)) {
      return true;
    }

    if ((this.getType() == null || !this.getType().equals(other.getType()))) {
      return false;
    }

    return Arrays.equals(this.getConfig(), other.getConfig());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Source source = (Source) o;
    return this.equalTo(source);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  public String getTypeString() {
    return this.getType().getValueDescriptor().getName();
  }
}
