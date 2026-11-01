#!/usr/bin/env python3
"""Replace the BDAS block in the HPK server script with a new version.

Usage (on the server):
    sudo python3 update_bdas.py /opt/hpk/server_hpk.py ~/server_bdas_addon.py

Makes a timestamped backup first. Works whether or not the BDAS block is
already in the file: an existing block (from its start comment up to the
end marker, or up to `if __name__ == "__main__":` for the first version,
which had no end marker) is replaced; otherwise the block is inserted above
`if __name__ == "__main__":`.
"""
import re, sys, shutil, time

START = "# ---------------- BDAS classroom view"
END = "# ---------------- end BDAS ----------------"
MAIN = 'if __name__ == "__main__":'

server, addon = sys.argv[1], sys.argv[2]
s = open(server).read()
a = open(addon).read().rstrip() + "\n\n\n"

backup = f"{server}.bak-{time.strftime('%Y%m%d-%H%M%S')}"
shutil.copy2(server, backup)

# Match markers only at the START of a line: the add-on's own header comment
# mentions the __main__ line, and that mention must never count as the real one.
main_re = re.compile(r'^if __name__ == "__main__":', re.M)
end_re = re.compile(r'^' + re.escape(END) + r'\n*', re.M)
start_re = re.compile(r'^' + re.escape(START), re.M)

m_start = start_re.search(s)
if m_start:
    i = m_start.start()
    m_end = end_re.search(s, i)
    if m_end:
        j = m_end.end()
    else:
        m_main = main_re.search(s, i)
        assert m_main, "no __main__ line after the BDAS block"
        j = m_main.start()
    s = s[:i] + a + s[j:]
    print("BDAS block replaced")
else:
    m_main = main_re.search(s)
    assert m_main, "could not find the __main__ line"
    s = s[:m_main.start()] + a + s[m_main.start():]
    print("BDAS block inserted")

open(server, "w").write(s)
print("backup:", backup)
