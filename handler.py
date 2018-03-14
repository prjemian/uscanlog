#!/usr/bin/env python

import json
import lxml
import os
import sys


def main():
    for fname in os.listdir("."):
        if fname.endswith(".xml"):
            print(fname)


if __name__ == "__main__":
    main()
