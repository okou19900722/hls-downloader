package tk.okou.hls;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HLSDownloaderVerticle extends AbstractVerticle {
    int size = 0;
    private String name = "小镇车神";
    private String output = "H:\\okou\\movie";
    private OpenOptions options = new OpenOptions();

    @Override
    public void start() {
        String outputPath = output + "\\" + name;
        File f = new File(outputPath);
        if (!f.exists()) {
            boolean mkdirs = f.mkdirs();
            if (!mkdirs) {
                throw new RuntimeException("创建目录[" + outputPath + "]失败");
            }
        }
        String m3u8Url = "https://bilibili.com-h-bilibili.com/20190412/8400_030d1cfc/1000k/hls/index.m3u8";
        String baseUrl = m3u8Url.substring(0, m3u8Url.lastIndexOf("/") + 1);
        System.err.println(baseUrl);
        WebClientOptions options = new WebClientOptions();
        options.setTrustAll(true);
        options.setMaxPoolSize(100);
        WebClient client = WebClient.create(vertx, options);
        HttpRequest<Buffer> request = client.getAbs(m3u8Url);
        request.as(BodyCodec.string()).send(r -> {
            if (r.failed()) {
                r.cause().printStackTrace();
            } else {
                String result = r.result().body();
                String[] strs = result.split("\n");
                List<String> list = Stream.of(strs).filter(line -> !line.startsWith("#") && line.endsWith(".ts")).collect(Collectors.toList());
                this.size = list.size();
                System.out.println("需要下载资源数：" + this.size + "");
                List<Future> futures = new ArrayList<>(this.size);
                for (int i = 0; i < list.size(); i++) {
                    String url = baseUrl + list.get(i);
                    try {
                        Future<Void> future = Future.future();
                        request(client, url, outputPath + "\\" + name + i + ".ts", future);
                        futures.add(future);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                CompositeFuture.all(futures).setHandler(cr -> {
                    if (cr.failed()) {
                        cr.cause().printStackTrace();
                    } else {
                        System.out.println("下载结束，开始合并文件");

                        String finalName = output + "\\" + name + ".mp4";
                        vertx.fileSystem().open(finalName, this.options, r3 -> {
                            if (r3.failed()) {
                                System.err.println("打开文件[" + finalName + "]失败！");
                            } else {
                                AsyncFile af = r3.result();
                                Future<Void> f1 = Future.future();
                                compose(af, outputPath, 0, f1);
                                f1.setHandler(r2 -> {
                                    if (r2.failed()) {
                                        System.err.println("合并失败");
                                        r2.cause().printStackTrace();
                                    } else {
                                        System.out.println("合并文件结束");
                                        vertx.close();
                                    }
                                });
                            }
                        });

                    }
                });
            }
        });
    }

    private void compose(AsyncFile asyncFile, String outputPath, int currentIndex, Future<Void> future) {
        if (currentIndex == this.size) {
            future.complete();
            return;
        }
        System.out.println("合并第：" + currentIndex + "个文件");
        String fileName = outputPath + "\\" + name + currentIndex + ".ts";
        FileSystem fs = vertx.fileSystem();
        fs.open(fileName, options, r -> {
            if (r.failed()) {
                future.fail(r.cause());
            } else {
                r.result().pipe().endOnSuccess(false).to(asyncFile, ar -> {
                    if (ar.failed()) {
                        future.fail(ar.cause());
                    } else {
                        compose(asyncFile, outputPath, currentIndex + 1, future);
                    }
                });
            }
        });
    }

    private void request(WebClient client, String url, String fileName, Future<Void> future) throws IOException {
        request(client, url, fileName, future, 5);
    }

    private void retry(WebClient client, String url, String fileName, Future<Void> future, Throwable e, int retryNum) {
        if (retryNum == 0) {
            future.fail(e);
        } else {
            request(client, url, fileName, future, retryNum - 1);
        }
    }

    private void request(WebClient client, String url, String fileName, Future<Void> future, int retryNum) {
        client.getAbs(url).send(r -> {
            if (r.failed()) {
                retry(client, url, fileName, future, r.cause(), retryNum);
            } else {
                Buffer buffer = r.result().body();
                vertx.fileSystem().writeFile(fileName, buffer, r1 -> {
                    if (r1.failed()) {
                        retry(client, url, fileName, future, r1.cause(), retryNum);
                    } else {
                        future.complete();
                        System.out.println("下载：" + fileName + "成功");
                    }
                });
            }
        });
    }

    public static void main(String[] args) {
        System.setProperty("vertx.disableDnsResolver", "true");
        Launcher.main(new String[]{"run", HLSDownloaderVerticle.class.getName()});
    }


}
