# HAI Platform 官方文档生成

HAI Platform 官方文档，使用 [Sphinx](https://www.sphinx-doc.org/en/master/) 工具生成。

## 依赖
+ HAI Platform
+ haiscale
+ Python >= 3.8.5
+ Sphinx == 4.5.0
+ myst-parser == 0.16.1


## 使用说明

1. 按照[说明](https://github.com/HFAiLab/hai-platform) 完成 [HAI Platform](https://github.com/HFAiLab/hai-platform) 的安装；

2. 进入 HAI Platform 环境，安装 Sphinx 依赖：

    ```
    pip install sphinx sphinx_rtd_theme myst-parser sphinx-click sphinxcontrib.httpdomain sphinx_copybutton
    ```

    将 `sphinx_click_hfai/ext.py` 替换所安装的 `sphinx-click` 模块下的 `ext.py`；

3. 执行脚本 `build.sh`, 生成文档 `./docs/build/html`；

4. 搭建文件服务器，预览效果：

    ```
    cd docs/build/html
    python -m  http.server 8000
    ```