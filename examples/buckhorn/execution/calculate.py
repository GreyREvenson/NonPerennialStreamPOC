import os,sys
src = r'..\..\..\src'  # edit this path if necessary
sys.path.append(os.path.abspath(src))
import twtnamelist,twtdomain,twtwt,twttopo,twtsoils,twtstreams,twtcalc

fname_namelist = '../namelist.yaml' # edit this path if necessary
namelist = twtnamelist.Namelist(filename=fname_namelist)

twtdomain.set_domain(namelist)
twttopo.calc_topo_main(namelist)
twtsoils.set_soils_main(namelist)
twtstreams.set_streams_main(namelist)
twtwt.set_wtd_main(namelist)
twtcalc.calc_parflow_inundation(namelist)
twtcalc._calc_parflow_inundation_summary(namelist)