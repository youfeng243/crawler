# encoding=utf-8
import requests
while(True):
    try:
        # 北京
        # resp = requests.get('http://qyxy.baic.gov.cn/CheckCodeCaptcha?currentTimeMillis=1474614781702')
        # 广东
        # resp = requests.get('http://gsxt.gdgs.gov.cn/aiccips/verify.html?random=0.9824755052492311')
        # 吉林  http://211.141.74.198:8081/aiccips/securitycode?0.09200985128106498
        # 内蒙古 http://www.nmgs.gov.cn:7001/aiccips/verify.html?random=0.3192687486939365
        # 甘肃  http://xygs.gsaic.gov.cn/gsxygs/securitycode.jpg?v=1474883481828
        # resp = requests.get('http://xygs.gsaic.gov.cn/gsxygs/securitycode.jpg?v=1474883481828')
        #四川 resp = requests.get('http://gsxt.scaic.gov.cn/ztxy.do?method=createYzm3&dt=1474886609124&random=1474886609124')
        # 新疆 'http://gsxt.xjaic.gov.cn:7001/ztxy.do?method=createYzm&dt=1474886750562&random=1474886750562'
        # 陕西  http://xygs.snaic.gov.cn/ztxy.do?method=createYzm&dt=1474887067382&random=1474887067382
        # 山西 http://gsxt.sxaic.gov.cn/validateCode.jspx?type=0&id=0.5503966638442384
        # 江苏 http://www.jsgsj.gov.cn:58888/province/rand_img.jsp?type=7&temp=Tue%20Sep%2027%202016%2013:47:20%20GMT+0800%20(CST)
        # 山东 http://218.57.139.24/securitycode?0.8518926554926457
        # 辽宁 http://gsxt.lngs.gov.cn/saicpub/commonsSC/loginDC/securityCode.action?tdate=43488
        # 天津 http://tjcredit.gov.cn/verifycode?date=1474957009683
        # 广西 http://gxqyxygs.gov.cn/validateCode.jspx?type=0&id=0.438829069085397
        # 河北 http://www.hebscztxyxx.gov.cn/notice/captcha?preset=&ra=0.7730336950092546
        # 宁夏 http://gsxt.ngsh.gov.cn/ECPS/verificationCode.jsp?_=1474957432199
        # 重庆 http://gsxt.cqgs.gov.cn/sc.action?width=130&height=40&fs=23&t=1474957572564
        # 湖南 http://gsxt.hnaic.gov.cn/notice/captcha?preset=&ra=0.326631803765721
        # 贵州 http://gsxt.gzgs.gov.cn/search!generateCode.shtml?validTag=searchImageCode&1474957877275
        # 山西 http://gsxt.sxaic.gov.cn/validateCode.jspx?type=0&id=0.6620452323385916
        resp = requests.get('http://gsxt.sxaic.gov.cn/validateCode.jspx?type=0&id=0.6620452323385916')
        file = {"cap_image": resp.content}
        data = {
            'province': 'shanxicu',
        }
        result = requests.post('http://123.56.217.114:8080', files=file, data=data)
        print result
        status=result.headers['status']
        province = result.headers['province']
        id = result.headers['id']
        code = result.content
        print code
    except Exception as e:
        print e.message