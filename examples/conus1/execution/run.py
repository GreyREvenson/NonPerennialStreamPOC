import os,sys,asyncio
src = r'..\..\..\src'
sys.path.append(os.path.abspath(src))
import twtmain
if __name__ == "__main__":
    asyncio.run(twtmain.calculate(fname_namelist=sys.argv[1:][0],
                                  fname_verbose=sys.argv[1:][1]))