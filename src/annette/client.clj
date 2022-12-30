(ns annette.client
  (:import (io.netty.bootstrap Bootstrap)
           (io.netty.buffer Unpooled)
           (io.netty.channel ChannelInitializer ChannelOption SimpleChannelInboundHandler)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.channel.socket.nio NioSocketChannel)
           (io.netty.util CharsetUtil)))

(defn echo-client-handler
  []
  (proxy [SimpleChannelInboundHandler] []
    (channelActive [ctx]
      (let [msg (Unpooled/copiedBuffer "Hello world!" CharsetUtil/UTF_8)]
        (.writeAndFlush ctx msg)))
    (channelRead0 [_ctx in]
      (println (.toString in CharsetUtil/UTF_8)))
    (exceptionCaught [ctx e]
      (.printStackTrace e)
      (.close ctx))))

(defn run
  [host port]
  (let [worker-group (NioEventLoopGroup.)]
    (try
      (let [b (Bootstrap.)]
        (-> b
            (.group worker-group)
            (.channel NioSocketChannel)
            (.option ChannelOption/SO_KEEPALIVE true)
            (.handler
              (proxy [ChannelInitializer] []
                (initChannel [ch] (-> ch
                                      (.pipeline)
                                      (.addLast (echo-client-handler)))))))
        (let [f (-> b
                    (.connect host port)
                    (.sync))]
          (-> f
              (.channel)
              (.closeFuture)
              (.sync))))
      (finally
        (.shutdownGracefully worker-group)))))
