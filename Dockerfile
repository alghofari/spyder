FROM python:3.9.15-slim

RUN apt-get clean
RUN apt-get update --fix-missing
RUN apt-get install -y gconf-service \
    libasound2 \
    libatk1.0-0 \
    libcairo2 \
    libcups2 \
    libfontconfig1 \
    libgdk-pixbuf2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libpango-1.0-0 \
    libxss1 \
    fonts-liberation \
    libappindicator1 \
    libnss3 \
    lsb-release \
    xdg-utils \
    wget \
    xvfb \
    tzdata \
    libnss3-tools

# check versioning : https://www.ubuntuupdates.org/package/google_chrome/stable/main/base/google-chrome-stable

RUN wget https://mirror.cs.uchicago.edu/google-chrome/pool/main/g/google-chrome-stable/google-chrome-stable_109.0.5414.119-1_amd64.deb
RUN dpkg -i google-chrome-stable_109.0.5414.119-1_amd64.deb; apt-get -fy install

# Download and install Firefox
RUN wget https://download-installer.cdn.mozilla.net/pub/firefox/releases/114.0.2/linux-x86_64/en-US/firefox-114.0.2.tar.bz2 \
    && apt-get install -y wget bzip2 \
    && tar xjf firefox-114.0.2.tar.bz2 \
    && mv firefox /opt \
    && ln -s /opt/firefox/firefox /usr/local/bin/firefox

# Download and install geckodriver
RUN wget https://github.com/mozilla/geckodriver/releases/download/v0.33.0/geckodriver-v0.33.0-linux64.tar.gz \
    && tar -xvzf geckodriver-v0.33.0-linux64.tar.gz \
    && mv geckodriver /usr/local/bin/

RUN apt install -y openvpn  openresolv  libgbm1
RUN wget "https://raw.githubusercontent.com/ProtonVPN/scripts/master/update-resolv-conf.sh" -O "/etc/openvpn/update-resolv-conf"
RUN chmod +x "/etc/openvpn/update-resolv-conf"

WORKDIR /app
RUN mkdir -p assets/browser
RUN cp /usr/local/bin/geckodriver /app/assets/browser/geckodriver

COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip
RUN pip install --user -r requirements.txt

RUN python -m seleniumwire extractcert
RUN mkdir -p $HOME/.pki/nssdb
RUN certutil -d sql:$HOME/.pki/nssdb -A -t TC -n "Selenium Wire" -i ca.crt

COPY executor executor
COPY extract extract
COPY helpers helpers
COPY transform transform
COPY utils utils
COPY main.py main.py

ENTRYPOINT ["python", "main.py"]