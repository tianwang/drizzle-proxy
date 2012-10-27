#!/bin/bash

gcc -g demo.c -I/usr/local/drizzle-proxy/include -L/usr/local/drizzle-proxy/lib /usr/local/drizzle-proxy/lib/libev.a -I/usr/local/drizzle/include/libdrizzle-1.0 -L/usr/local/drizzle/lib /usr/local/drizzle/lib/libdrizzle.so -Wl,--rpath -Wl,/usr/local/drizzle/lib -D__STDC_FORMAT_MACROS -lstdc++ 
