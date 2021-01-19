/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.hydrator.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.soulwing.rot13.Rot13;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * Hydrator Transform Plugin Example - This provides a good starting point for building your own Transform Plugin
 * For full documentation, check out: https://docs.cask.co/cdap/current/en/developer-manual/pipelines/developing-plugins/index.html
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("ExampleSink") // <- NOTE: The name of the plugin should match the name of the docs and widget json files.
@Description("This is an example sink.")
public class ExampleSink extends Transform<StructuredRecord, StructuredRecord> {
  // If you want to log things, you will need this line
  private static final Logger LOG = LoggerFactory.getLogger(ExampleSink.class);

  // Usually, you will need a private variable to store the config that was passed to your class
  private final Config config;
  private Schema outputSchema;

  public ExampleSink(Config config) {
    this.config = config;
  }

  /**
   * This function is called when the pipeline is published. You should use this for validating the config and setting
   * additional parameters in pipelineConfigurer.getStageConfigurer(). Those parameters will be stored and will be made
   * available to your plugin during runtime via the TransformContext. Any errors thrown here will stop the pipeline
   * from being published.
   * @param pipelineConfigurer Configures an ETL Pipeline. Allows adding datasets and streams and storing parameters
   * @throws IllegalArgumentException If the config is invalid.
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    // It's usually a good idea to validate the configuration at this point. It will stop the pipeline from being
    // published if this throws an error.
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    config.validate(inputSchema);
    try {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
    } catch (IOException e) {
      throw new IllegalArgumentException("Output schema cannot be parsed.", e);
    }
  }

  /**
   * This function is called when the pipeline has started. The values configured in here will be made available to the
   * transform function. Use this for initializing costly objects and opening connections that will be reused.
   * @param context Context for a pipeline stage, providing access to information about the stage, metrics, and plugins.
   * @throws Exception If there are any issues before starting the pipeline.
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    outputSchema = Schema.parseJson(config.schema);
  }

  /**
   * This is the method that is called for every record in the pipeline and allows you to make any transformations
   * you need and emit one or more records to the next stage.
   * @param input The record that is coming into the plugin
   * @param emitter An emitter allowing you to emit one or more records to the next stage
   * @throws Exception
   */
  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    // Get all the fields that are in the output schema
    List<Schema.Field> fields = outputSchema.getFields();
    // Create a builder for creating the output record
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    // Add all the values to the builder
    for (Schema.Field field : fields) {
      String name = field.getName();
      if (input.get(name) != null) {
        String inputValue = input.get(name).toString();
    String outputValue = Rot13.rotate(inputValue);
        builder.set(name, outputValue);
      }
    }
    // If you wanted to make additional changes to the output record, this might be a good place to do it.

    // Finally, build and emit the record.
    emitter.emit(builder.build());
  }

  /**
   * This function will be called at the end of the pipeline. You can use it to clean up any variables or connections.
   */
  @Override
  public void destroy() {
    // No Op
  }

  /**
   * Your plugin's configuration class. The fields here will correspond to the fields in the UI for configuring the
   * plugin.
   */
  public static class Config extends PluginConfig {

    @Name("schema")
    @Description("Specifies the schema of the records outputted from this plugin.")
    private final String schema;

    @Name("scrambleText")
    @Description("Field containing text to be scrambled.")
    @Macro // <- Macro means that the value will be substituted at runtime by the user.
    @Nullable //  <- Indicates that the config param is optional
    private final String scrambleText;

    public Config(String myOption, Integer myOptionalOption, String inputField, String schema) {
      this.schema = schema;
      this.scrambleText = inputField;
    }

    private void validate(Schema inputSchema) throws IllegalArgumentException {
      // It's usually a good idea to check the schema. Sometimes users edit
      // the JSON config directly and make mistakes.
      try {
        Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Output schema cannot be parsed.", e);
      }
      // This method should be used to validate that the inputField is valid.
      if (scrambleText == null || scrambleText.isEmpty()) {
        throw new IllegalArgumentException("scrambleText is a required field.");
      }
      // You can use the containsMacro() function to determine if you can validate at deploy time or runtime.
      // If your plugin depends on fields from the input schema being present or the right type, use inputSchema
    }
  }
}

