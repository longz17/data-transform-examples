package io.cdap.example.plugin;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchActionContext;
import io.cdap.cdap.etl.api.batch.PostAction;
import org.apache.twill.filesystem.Location;

import java.util.regex.Pattern;

/**
 * Post run action that deletes files in a FileSet that match a configurable regex.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(FilesetDeletePostAction.NAME)
@Description("Post run action that deletes files in a FileSet that match a configurable regex if the run succeeded.")
public class FilesetDeletePostAction extends PostAction {
    public static final String NAME = "FilesetDelete";
    private final Conf config;

    /**
     * Config properties for the plugin.
     */
    public static class Conf extends PluginConfig {
        public static final String FILESET_NAME = "filesetName";
        public static final String DELETE_REGEX = "deleteRegex";
        public static final String DIRECTORY = "directory";

        @Name(FILESET_NAME)
        @Description("The fileset to delete files from.")
        private String filesetName;

        @Name(DELETE_REGEX)
        @Description("Delete files that match this regex.")
        private String deleteRegex;

        // Macro enabled properties can be set to a placeholder value ${key} when the pipeline is deployed.
        // At runtime, the value for 'key' can be given and substituted in.
        @Macro
        @Name(DIRECTORY)
        @Description("The fileset directory to delete files from.")
        private String directory;
    }

    public FilesetDeletePostAction(Conf config) {
        this.config = config;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        Pattern.compile(config.deleteRegex);
    }

    @Override
    public void run(BatchActionContext context) throws Exception {
        if (!context.isSuccessful()) {
            return;
        }

        FileSet fileSet = context.getDataset(config.filesetName);
        Pattern pattern = Pattern.compile(config.deleteRegex);
        for (Location fileLocation : fileSet.getBaseLocation().append(config.directory).list()) {
            if (pattern.matcher(fileLocation.getName()).find()) {
                fileLocation.delete();
            }
        }
    }
}