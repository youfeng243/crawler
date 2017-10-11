#!/usr/bin/env python
import os
import sys

from i_util.tools import get_project_path
from i_extractor.libs.extract_exception import NoFoundPluginException

plugin_base_path = "/".join([get_project_path(), "i_extractor", "plugins"])

class PluginHandler():
    def get_plugin_module_info(self, plugin_names):
        plugin_infos = []
        for plugin_name in plugin_names.split(";"):
            if len(plugin_name) < 3:continue
            plugin_dir_path = plugin_base_path+"/" + plugin_name
            if not os.path.exists(plugin_dir_path):
                raise Exception("plugin {} no found".format(plugin_name))
            dirs = os.listdir(plugin_base_path + "/" + plugin_name)
            plugin_file = None
            for f in dirs:
                if f.endswith("plugin.py"):
                    plugin_file = f
                    break

            if not plugin_file:
                return [plugin_name, None]
            plugin_entry_name = plugin_file.replace(".py", "")
            plugin_infos.append([plugin_name, plugin_entry_name])
        return plugin_infos

    def get_plugin_entry(self, plugin_info):
        if not plugin_info[1]:
            raise NoFoundPluginException()
        module_dir_name = "plugins.{}".format(plugin_info[0])
        if sys.modules.has_key(module_dir_name):
            return getattr(sys.modules[module_dir_name], plugin_info[1]).extract
        raise NoFoundPluginException()

    def load_plugin_module(self, plugin_info):
        module_dir_name = "plugins.{}".format(plugin_info[0])
        if sys.modules.has_key(module_dir_name):
            for m in sys.modules:
                module_name = m.split(".")
                if len(module_name) < 2:continue
                if module_name[0] == "plugins" and module_name[1] == plugin_info[0] and sys.modules.get(m):
                    reload(sys.modules[m])
        importstring = "from plugins.{} import {}".format(plugin_info[0], plugin_info[1])
        exec importstring

if __name__ == "__main__":
    ph = PluginHandler()
