// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <net/if.h>

#include <iostream>
#include <cstring>
#include <stdexcept>

#include "monitor/interface.h"
#include "monitor/handler.h"
#include "monitor/config.h"

int Interface::initInterface(const char * dev) {
    this->dev_ = strdup(dev);

    if (dev_ == NULL)
        goto out;

    int sock;
    struct ifreq iFreq;
    struct sockaddr_in *saddr;

    if ((sock=socket(AF_INET, SOCK_RAW, htons(0x0806))) < 0) {
        std::cerr << "creating socket failed, are you root?" << std::endl;
        goto out;
    }

    strcpy(iFreq.ifr_name, dev);

    if (ioctl(sock, SIOCGIFADDR, &iFreq) < 0) {
        std::cerr << "ioctl failed on the selected device" << std::endl;
        goto out;
    }

    saddr = (struct sockaddr_in *) &iFreq.ifr_addr;

    this->str_ = (char *) malloc (16 * sizeof(char));

    if (this->str_ == NULL)
        goto out;

    this->addr_ = saddr->sin_addr.s_addr;
    this->saFamily_ = AF_INET;
    inet_ntop(AF_INET, &addr_, this->str_, 15);

    this->handler_ = new PacketHandler(dev_);
    if (handler_->openDevLive() < 0) {
        std::cout << "couldn't open device " <<
          this->dev_ << " for packet capture\n";
        goto out;
    }

    if (handler_->setNonBlock() < 0) {
        std::cout << "couldn't set the device to nonblock mode."
          << " Don't panic, we can still proceed...\n";
    }

    return 0;

out:
    return -1;
}

char * Interface::getAddr() {
    return this->str_;
}

Interface::~Interface() {
    delete this->handler_;
    if (dev_)
        free(dev_);
    if (str_)
        free(str_);
}

bool Interface::contains(const in_addr_t & addr) {
#ifdef PERF_DEBUG
    // std::cout << addr << " " << this->addr_ << std::endl;
#endif
    if ((this->saFamily_ == AF_INET) && (addr == this->addr_))
        return true;
    return false;
    // return next_->contains(addr);
}

int Interface::processNextPkt() {
    return this->handler_->dispatch(-1);
}
