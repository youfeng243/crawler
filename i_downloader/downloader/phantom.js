var port, server, service,
  system = require('system'),
  webpage = require('webpage');

if (system.args.length < 3) {
  console.log('Usage: simpleserver.js <portnumber> <servertype>');
  phantom.exit(1);
} else {
  port = system.args[1];
  server_type = system.args[2];
  server_proxy = 'localhost';
  server = require('webserver').create();
  console.debug = function(){};
    //监听请求调用function
  service = server.listen(port, {
    'keepAlive': false
  }, function (request, response) {
  //清空cookie
    phantom.clearCookies();

    console.log('request:');
    console.log(JSON.stringify(request, null, 4));
    // check method
    if (request.method == 'GET') {
        if (request.url == '/settings') {
            body = 'server_type: ' + server_type + '\nserver_proxy: ' + server_proxy;
            response.statusCode = 200;
        } else {
            body = 'url or method error!';
            response.statusCode = 403;
        }
        response.headers = {
            'Cache': 'no-cache',
            'Content-Length': body.length
        };
        response.write(body);
        response.closeGracefully();
        return;
    } else {
        //get request body
        try {
            var fetch = request.post;
            console.log(fetch.url)
            //console.log(JSON.stringify(request, null, 2));
            //var fetch = JSON.parse(request.postRaw);
        } catch (e) {
            body = 'post data must be json string';
            logger_log(body);
            response.headers = {
                'Cache': 'no-cache',
                'Content-Length': body.length
            };
            response.write(body);
            response.closeGracefully();
            return;
        }
    }

    /*
    if (request.url == '/settings') {
        body = '';
        if ('server_type' in fetch){
            server_type = fetch['server_type'];
            body += 'server_type: ' + server_type + '\n';
        }
        if ('proxy' in fetch) {
            proxy = JSON.parse(fetch.proxy);
            server_proxy = proxy['host'] + ':' + proxy['port'];
            phantom.setProxy(proxy['host'], proxy['port'],
                'manual', proxy['username'], proxy['password']);
            body += server_proxy + '\n';
            console.log('using proxy: ' + proxy);
        }
        response.statusCode = 200;
        response.headers = {
            'Cache': 'no-cache',
            'Content-Length': body.length
        };
        response.write(body);
        response.closeGracefully();
        logger_log(body);
        return;
    }
    */

    //set proxy
    //if (server_type == 'dynamic' && 'proxy' in fetch) {
    //获取代理
    if ('proxy' in fetch) {
        proxy = JSON.parse(fetch.proxy);
        console.log('change proxy: ' + fetch.proxy);
        phantom.setProxy(proxy.host, proxy.port, 'manual', proxy.user, proxy.password);
    } else {
        phantom.setProxy("", 0, "manual", "", "");
    }

    var first_response = null,
        finished = false,
        page_loaded = false,
        start_time = Date.now(),
        end_time = null,
        script_executed = false,
        script_result = null,
        wait_before_end = fetch.wait_before_end || 1000;

    //var fetch = JSON.parse(request.postRaw);
    console.debug(JSON.stringify(fetch, null, 2));

    // create and set page
    //创建page
    var page = webpage.create();
    //解析头信息
    var header = JSON.parse(fetch.headers);
    if (header && header.Cookie) {
        page.addCookie({domain: fetch.domain,
                      expires: 'Wed, 30 Aug 2017 16:57:46 GMT',
                      expiry: 1476128618,
                      httponly: false,
                      name: 'Cookie',
                      path: '/',
                      secure: false,
                      value: header.Cookie});
    }
    if (header && header['User-Agent']) {
       page.settings.userAgent = header['User-Agent'];
    }
    page.onConsoleMessage = function(msg) {
        //console.log('console: ' + msg);
    };
    page.viewportSize = {
      width: fetch.js_viewport_width || 1024,
      height: fetch.js_viewport_height || 768*3
    }
    if (fetch.headers && fetch.headers['User-Agent']) {
        page.settings.userAgent = fetch.headers['User-Agent'];
    }
    // this may cause memory leak: https://github.com/ariya/phantomjs/issues/12903
    page.settings.loadImages = fetch.load_images === undefined ? false: fetch.load_images;
    page.settings.resourceTimeout = fetch.timeout ? fetch.timeout * 1000 : 60 * 1000;
    if (fetch.headers) {
        headers = JSON.parse(fetch.headers);
        page.customHeaders = headers;
    }
    console.log('')
    // add callbacks
    //page初始化函数
    page.onInitialized = function() {
      if (!script_executed && fetch.js_script && fetch.js_run_at === "document-start") {
        script_executed = true;
        console.log('running document-start script.');
        script_result = page.evaluateJavaScript(fetch.js_script);
      }
    };
    //page加载完成函数
    page.onLoadFinished = function(status) {
      page_loaded = true;
      if (!script_executed && fetch.js_script && fetch.js_run_at !== "document-start") {
        script_executed = true;
        console.log('running document-end script.');
        script_result = page.evaluateJavaScript(fetch.js_script);
      }
      console.log("waiting "+wait_before_end+"ms before finished.");
      end_time = Date.now() + wait_before_end;
      setTimeout(make_result, wait_before_end+10, page);
    };
    //page资源请求函数
    page.onResourceRequested = function(request) {

      console.log("Starting request: #"+request.id+" ["+request.method+"]"+request.url);
      end_time = null;
    };
    //page资源接收函数
    page.onResourceReceived = function(response) {
      console.log('Request finished ----------')
      console.log("Request finished: #"+response.id+" ["+response.status+"]"+response.url);
      if (first_response === null && response.status != 301 && response.status != 302) {
        first_response = response;
      }
      if (page_loaded) {
        console.log("waiting "+wait_before_end+"ms before finished.");
        end_time = Date.now() + wait_before_end;
        setTimeout(make_result, wait_before_end+10, page);
      }
    }
    //page资源接收失败函数
    page.onResourceError = page.onResourceTimeout=function(response) {
      console.log('Request finished ----------')
      console.log("Request error: #"+response.id+" ["+response.errorCode+"="+response.errorString+"]"+response.url);
      if (first_response === null) {
        first_response = response;
      }
      if (page_loaded) {
        console.log("waiting "+wait_before_end+"ms before finished.");
        end_time = Date.now() + wait_before_end;
        setTimeout(make_result, wait_before_end+10, page);
      }
    }

    // make sure request will finished
    //全局超时设置
    setTimeout(function(page) {
      make_result(page);
    }, page.settings.resourceTimeout + 100, page);

    // send request
    //发送请求
    console.log('page open url')
    page.open(fetch.url, {
      operation: fetch.method,
      data: fetch.data,
    });

    // make response
    //得到结果
    function make_result(page) {
      if (finished) {
        return;
      }
      if (Date.now() - start_time < page.settings.resourceTimeout) {
        if (!!!end_time) {
          return;
        }
        if (end_time > Date.now()) {
          setTimeout(make_result, Date.now() - end_time, page);
          return;
        }
      }

      var result = {};
      try {
        result = _make_result(page);
      } catch (e) {
        result = {
          orig_url: fetch.url,
          status_code: 599,
          error: e.toString(),
          content:  '',
          headers: {},
          url: page.url,
          cookies: {},
          time: (Date.now() - start_time) / 1000,
          save: fetch.save
        }
      }

      page.close();
      finished = true;
      console.log("["+result.status_code+"] "+result.orig_url+" "+result.time)

        try {
            var body = JSON.stringify(result, null, 2);
            response.writeHead(200, {
                'Cache': 'no-cache',
                'Content-Type': 'application/json',
            });
            response.write(body);
            response.closeGracefully();
      } catch (e) {
            return;
      }
    }

    function _make_result(page) {
      if (first_response === null) {
        throw "No response received!";
      }

      var cookies = {};
      page.cookies.forEach(function(e) {
        cookies[e.name] = e.value;
      });

      var headers = {};
      if (first_response.headers) {
        first_response.headers.forEach(function(e) {
          headers[e.name] = e.value;
        });
      }

      return {
        orig_url: fetch.url,
        status_code: first_response.status || 599,
        error: first_response.errorString,
        content:  page.content,
        headers: headers,
        url: page.url,
        cookies: cookies,
        time: (Date.now() - start_time) / 1000,
        js_script_result: script_result,
        save: fetch.save
      }
    }
  });

  if (service) {
    console.log('Phantom server running on port ' + port);
  } else {
    console.log('Error: Could not create phantom server listening on port ' + port);
    phantom.exit();
  }
}
