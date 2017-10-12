# -*- coding: utf-8 -*-

from app import create_app
from conf import SERVER, log

application = create_app()

if __name__ == '__main__':
    log.info("start_i_manager_server")
    application.run(
        host=SERVER.get('host', '0.0.0.0'),
        port=SERVER.get('port', 8919),
        debug=SERVER.get('debug', True)
    )
