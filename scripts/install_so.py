import os
import argparse
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

注意：wtpy的so文件和wtcpp的so文件并不同步。例如，对于libWtBtPorter.so而言，wtcpp比wtpy多一个boost依赖
'''

def install_so(srcPath:str, dstPath:str):
    dir = os.walk(srcPath)

    for p, dir_list, file_list in dir:
        for file in file_list:
            if file == 'libCTPLoader.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")  
                
            # add libTraderDumper.so for wtpy v0.9.3
            elif file == 'libTraderDumper.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")

            elif file == 'libCTPOptLoader.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")  
            elif file == 'libMiniLoader.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")     
            elif file == 'libWtBtPorter.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")   
            elif file == 'libWtDataStorage.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")  
            elif file == 'libWtDataStorageAD.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")  
            elif file == 'libWtDtHelper.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")   
            elif file == 'libWtDtPorter.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")   
            elif file == 'libWtDtServo.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")   
            elif file == 'libWtExecMon.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")   
            elif file == 'libWtMsgQue.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")  
            elif file == 'libWtPorter.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")  
            elif file == 'libWtRiskMonFact.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")  
            elif file == 'libWtExeFact.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'executer', file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")  
            elif file == 'libParserCTP.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'parsers', file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")  
            elif file == 'libParserCTPMini.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'parsers', file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")  
            elif file == 'libParserCTPOpt.so':
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'parsers', file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")  
            elif file == 'libParserFemas.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'parsers', file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")    
            elif file == 'libParserUDP.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'parsers', file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")   
            elif file == 'libParserXTP.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'parsers', file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")   
            elif file == 'libTraderCTP.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'traders', file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")   
            elif file == 'libTraderCTPMini.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'traders', file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")    
            elif file == 'libTraderCTPOpt.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'traders', file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")    
            elif file == 'libTraderFemas.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'traders', file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")    
            elif file == 'libTraderMocker.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'traders', file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")  
            elif file == 'libTraderXTP.so' and (p.endswith('bin') or p.endswith('bin/')):
                ogn = os.path.join(p, file)
                tgt = os.path.join(dstPath, 'traders', file)
                shutil.copy(ogn, tgt)
                print(f"copy {ogn} to {tgt}")   
            

parser = argparse.ArgumentParser()
parser.add_argument('--source', '-s', help='源so文件存放目录', default='/Wondertrader/code/wtcpp/src/build_all/build_x64/Release/bin')
parser.add_argument('--destination', '-d', help='目标so文件存放目录', default='/Wondertrader/code/wtpy/wtpy/wrapper/linux')
args = parser.parse_args()


if __name__ == "__main__":

    install_so(args.source, args.destination)