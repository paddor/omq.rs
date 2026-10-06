package io.omq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class RuntimeCompatibilityTest {
    @Test
    void ringIoAndVirtualReceivesWorkWithOneCarrier() throws Exception {
        String bindingPath = Path.of(OMQ.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                .toString();
        String testPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        if (Runtime.version().feature() == 21) {
            command.add("--enable-preview");
        }
        command.addAll(List.of(
                "--enable-native-access=ALL-UNNAMED",
                "-Djdk.virtualThreadScheduler.parallelism=1",
                "-Djdk.virtualThreadScheduler.maxPoolSize=1",
                "-Djava.library.path=" + System.getProperty("java.library.path"),
                "-cp",
                bindingPath + File.pathSeparator + testPath,
                "io.omq.smoke.PackagingSmoke"));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        try {
            assertTrue(process.waitFor(15, TimeUnit.SECONDS), "runtime smoke timed out");
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(0, process.exitValue(), output);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly().waitFor();
            }
        }
    }
}
