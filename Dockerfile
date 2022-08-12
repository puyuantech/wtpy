FROM wtbase:0.2

LABEL name="wtapp"
LABEL maintainer="puyuan<github@puyuan.tech>"

ENV TZ=Asia/Shanghai

RUN cd /home/wondertrader && mkdir code && cd code && \
    echo -e "\\033[45;37m ############### wtcpp ############### \033[0m" && \
    git clone -b master https://hub.fastgit.xyz/puyuantech/wtcpp.git && \
    cd wtcpp/src && ./build_release.sh

RUN cd /home/wondertrader/code/ && \
    echo -e "\\033[45;37m ############### wtpy ############### \033[0m" && \
    git clone -b master https://hub.fastgit.xyz/puyuantech/wtpy.git && \
    cd wtpy/scripts && python3 install_so.py -s /home/wondertrader/code/wtcpp/src/build_all/build_x64/Release/bin -d /home/wondertrader/code/wtpy/wtpy/wrapper/linux &&\
    cd /home/wondertrader/code/wtpy && python3 setup.py install

WORKDIR /home/wondertrader

CMD ["/usr/sbin/init"]