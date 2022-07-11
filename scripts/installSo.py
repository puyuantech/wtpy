import os
import shutil

'''
用于将wtcpp编译得到的so文件精准安装到wtpy


不在编译过程中产生的so文件：
    libthostmduserapi.so
    libUSTPmduserapiAF.so
    libxtpquoteapi.so
    soptthostmduserapi_se.so
    thostmduserapi_se.so
    libthosttraderapi.so
    libUSTPtraderapiAF.so
    libxtptraderapi.so
    soptthosttraderapi_se.so
    thosttraderapi_se.so
'''

def install_so(srcPath:str, dstPath:str):
    dir = os.walk(srcPath)

    for p, dir_list, file_list in dir:
        for file in file_list:
            if file == 'libCTPLoader.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                print(ogn)
                print(tgt)
            elif file == 'libCTPOptLoader.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                print(ogn)
                print(tgt)
            elif file == 'libMiniLoader.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                print(ogn)
                print(tgt)    
            elif file == 'libWtBtPorter.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                print(ogn)
                print(tgt)   
            elif file == 'libWtDataStorage.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                print(ogn)
                print(tgt)
            elif file == 'libWtDataStorageAD.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                print(ogn)
                print(tgt)
            elif file == 'libWtDtHelper.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                print(ogn)
                print(tgt)   
            elif file == 'libWtDtPorter.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                print(ogn)
                print(tgt)   
            elif file == 'libWtDtServo.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                print(ogn)
                print(tgt)   
            elif file == 'libWtExecMon.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                print(ogn)
                print(tgt)   
            elif file == 'libWtMsgQue.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                print(ogn)
                print(tgt)
            elif file == 'libWtPorter.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                print(ogn)
                print(tgt)
            elif file == 'libWtRiskMonFact.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                print(ogn)
                print(tgt)
            elif file == 'libWtExeFact.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'executer', file)
                print(ogn)
                print(tgt)
            elif file == 'libParserCTP.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'parsers', file)
                print(ogn)
                print(tgt)
            elif file == 'libParserCTPMini.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'parsers', file)
                print(ogn)
                print(tgt)
            elif file == 'libParserCTPOpt.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'parsers', file)
                print(ogn)
                print(tgt)
            elif file == 'libParserFemas.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'parsers', file)
                print(ogn)
                print(tgt)   
            elif file == 'libParserUDP.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'parsers', file)
                print(ogn)
                print(tgt)   
            elif file == 'libParserXTP.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'parsers', file)
                print(ogn)
                print(tgt)   
            elif file == 'libTraderCTP.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'traders', file)
                print(ogn)
                print(tgt)   
            elif file == 'libTraderCTPMini.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'traders', file)
                print(ogn)
                print(tgt)   
            elif file == 'libTraderCTPOpt.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'traders', file)
                print(ogn)
                print(tgt)   
            elif file == 'libTraderFemas.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'traders', file)
                print(ogn)
                print(tgt)   
            elif file == 'libTraderMocker.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'traders', file)
                print(ogn)
                print(tgt)   
            elif file == 'libTraderXTP.so' and p.endswith('bin'):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'traders', file)
                print(ogn)
                print(tgt)   
            




if __name__ == "__main__":
    install_so('/home/hujiaye/Wondertrader/code/wtcpp/src/build_all/build_x64/Release/bin', '/home/hujiaye/Wondertrader/code/wtpy/wtpy/wrapper/linux')