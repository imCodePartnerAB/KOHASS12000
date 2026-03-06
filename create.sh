#!/bin/sh
VERSION=$(grep '^our $VERSION = "' Koha/Plugin/imCode/KohaSS12000/ExportUsers.pm | sed 's/.*"\(.*\)".*/\1/' | tr '.' '_')
zip -r "koha-plugin-export-users_ss12000_v${VERSION}.kpz" Koha
