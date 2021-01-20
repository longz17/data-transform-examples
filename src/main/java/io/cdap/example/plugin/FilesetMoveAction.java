package io.cdap.example.plugin;

import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import org.apache.twill.filesystem.Location;

import javax.annotation.Nullable;
import java.util.regex.Pattern;

/**
 * Action that moves files from one fileset into another, optionally filtering files that match a regex.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(FilesetMoveAction.NAME)
@Description("Action that moves files from one fileset into another, optionally filtering files that match a regex.")
public class FilesetMoveAction extends Action {
    public static final String NAME = "FilesetMove";
    private final Conf config;

    /**
     * Config properties for the plugin.
     */
    public static class Conf extends PluginConfig {
        public static final String SOURCE_FILESET = "sourceFileset";
        public static final String DEST_FILESET = "destinationFileset";
        public static final String FILTER_REGEX = "filterRegex";

        @Name(SOURCE_FILESET)
        @Description("The fileset to move files from.")
        private String sourceFileset;

        @Name(DEST_FILESET)
        @Description("The fileset to move files to.")
        private String destinationFileset;

        @Nullable
        @Name(FILTER_REGEX)
        @Description("Filter any files whose name matches this regex. Defaults to '^\\.', which will filter any files " +
                "that begin with a period.")
        private String filterRegex;

        // set defaults for properties in a no-argument constructor.
        public Conf() {
            filterRegex = "^\\.";
        }
    }

    public FilesetMoveAction(Conf config) {
        this.config = config;
    }

    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        Pattern.compile(config.filterRegex);
    }

    @Override
    public void run(ActionContext context) throws Exception {
        context.execute(new TxRunnable() {
            @Override
            public void run(DatasetContext context) throws Exception {
                FileSet sourceFileSet = context.getDataset(config.sourceFileset);
                FileSet destinationFileSet = context.getDataset(config.destinationFileset);

                Pattern pattern = Pattern.compile(config.filterRegex);

                for (Location sourceFile : sourceFileSet.getBaseLocation().list()) {
                    if (pattern.matcher(sourceFile.getName()).find()) {
                        continue;
                    }
                    Location destFile = destinationFileSet.getBaseLocation().append(sourceFile.getName());
                    sourceFile.renameTo(destFile);
                }
            }
        });
    }
}
