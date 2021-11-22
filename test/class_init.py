# -*- coding: utf-8 -*-
__metaclass__ = type


class aaa():
    def __init__(self):
        self.aa = 12345

    @classmethod
    def init_another(self, bb):
        self.bb = bb
        return aaa()
   
    bb = 110


if __name__ == '__main__':
    cc = aaa()
    print cc.bb
    print cc.aa
    dd = aaa(9)
    print cc is dd
    print cc.bb
    print cc.aa
    print dd.bb
    print dd.aa
