FROM ubuntu:16.04

ENV DEBIAN_FRONTEND noninteractive
ENV USER root

RUN apt-get update && apt-get install -y --no-install-recommends ubuntu-desktop

RUN apt-get update && \
    apt-get install -y gnome-panel gnome-settings-daemon metacity nautilus gnome-terminal  && \
    apt-get install -y tightvncserver && \
    mkdir /root/.vnc

RUN apt-get install -y openjdk-8-jdk

# Install Google Cloud SDK
RUN apt-get install -y curl
RUN export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN apt-get update && apt-get install -y google-cloud-sdk

RUN mkdir -p /root/.config/nautilus && \
    chmod 700 /root/.config/nautilus

ADD xstartup /root/.vnc/xstartup
RUN chmod 755 /root/.vnc/xstartup

ADD https://github.com/keighrim/mae-annotation/releases/download/v2.0.9/mae-2.0.9-fatjar.jar /root/
RUN chmod 755 /root/mae-2.0.9-fatjar.jar

# Can be overridden by adding `--env PASSWORD=mypassword` to the `docker run`
# command.
ENV PASSWORD password

CMD printf "$PASSWORD\n$PASSWORD\n\n" | vncpasswd && \
    export PASSWORD="" && \
    /usr/bin/vncserver :1 -geometry 1280x800 -depth 24 && tail -f /root/.vnc/*:1.log

EXPOSE 5901
