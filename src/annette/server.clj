(ns annette.server
  (:import (io.netty.bootstrap ServerBootstrap)
           (io.netty.buffer Unpooled)
           (io.netty.channel ChannelInitializer)
           (io.netty.channel ChannelFutureListener ChannelInboundHandlerAdapter ChannelOption)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.channel.socket.nio NioServerSocketChannel)))

(defn echo-server-handler []
  (proxy [ChannelInboundHandlerAdapter] []
    (channelRead [ctx msg]
      (.write ctx msg))
    (channelReadComplete [ctx]
      (-> ctx
          (.writeAndFlush Unpooled/EMPTY_BUFFER)
          (.addListener ChannelFutureListener/CLOSE)))
    (exceptionCaught [ctx e]
      (.printStackTrace e)
      (.close ctx))))

(defn start
  [port]
  (let [master-group (NioEventLoopGroup.)
        worker-group (NioEventLoopGroup.)]
    (try
      (let [b (ServerBootstrap.)]
        (-> b
            (.group master-group worker-group)
            (.channel NioServerSocketChannel)
            (.childHandler
              (proxy [ChannelInitializer] []
                (initChannel [ch] (-> ch
                                      (.pipeline)
                                      (.addLast (echo-server-handler))))))
            (.option ChannelOption/SO_BACKLOG (int 128))
            (.childOption ChannelOption/SO_KEEPALIVE true))
        (let [f (-> b
                    (.bind port)
                    (.sync))]
          (-> f
              (.channel)
              (.closeFuture)
              (.sync))))
      (finally
        (.shutdownGracefully worker-group)
        (.shutdownGracefully master-group)))))

(defn -main [& _args]
  (start 8080))
