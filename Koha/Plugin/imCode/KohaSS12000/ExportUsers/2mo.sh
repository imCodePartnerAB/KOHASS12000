#!/bin/bash
msgfmt sv.po -o sv.mo
cp sv.mo ./translations/sv/LC_MESSAGES/com.imcode.exportusers.mo
msgfmt fi.po -o fi.mo
cp fi.mo ./translations/fi/LC_MESSAGES/com.imcode.exportusers.mo
