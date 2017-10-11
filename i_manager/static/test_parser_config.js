new Vue({
    el: "#testParserConfigPage",
    data: {
        request_method: "get",
        request_url: "http://www.baidu.com",
        request_params: "",
        page_source:'cache',
        headers:"",
        parser_id: '-1',
        isLoading:false,
        download_type:'simple',
        is_save:'true',
        input_page:"",
        test_result: {
            "check_datas": "",
            "common_datas": {
                "content": "",
                "public_time": "",
                "title": ""
            },
            "datas": null,
            "entity_datas": [],
            "links": [],
            "response": "''",
            "status": {
                "content_language": null,
                "content_type": null,
                "download_elapsed": -1,
                "download_http_code": 0,
                "download_status_code": 1,
                "ex_status": 0,
                "extract_error": 0,
                "spend_time": {
                    "download_spend": 0,
                    "entity_spend": 0,
                    "extract_spend": 0
                },
                "src_type": null,
                "topic_id": null
            }
        }
    },
    methods: {
        testParserConfig: function ($event) {
            var datas = {
                'request_method': this.request_method,
                'request_url': this.request_url,
                'request_params': this.request_params,
                'parser_id': this.parser_id,
                'page_source':this.page_source,
                'headers':this.headers,
                'download_type':this.download_type,
                'is_save':this.is_save,
                'test_result':{}
            }
            if(this.page_source == 'input'){
                datas['input_page'] = this.input_page;
            }
            if(this.is_save == "true"){
                var r = confirm("该条数据会进行入库,是否确定入库?")
                if(r == false){
                    return ;
                }
            }

            var _self = this;
            $.ajax({
                url: '/api/parser/test_parser_config',
                type: 'POST',
                data: JSON.stringify(datas),
                dataType:'json',
                sync:true,
                contentType:"application/json;charset=utf-8",
                beforeSend:function () {
                    _self.isLoading = true;
                },
                success: function (data) {
                    if(data.status == true) {
                        _self.test_result = data.data;
                    }else{
                        alert(data.data)
                    }
                    _self.isLoading = false;
                },
                error:function () {
                    _self.isLoading = false;
                }
            })

        }
    }
})