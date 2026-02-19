import os,sys
import twtnamelist
import twtdomain
import twtwt
import twttopo
import twtsoils
import twtstreams
import twtcalc
import hf_hydrodata

async def runit(fname_namelist:str):

    fname_namelist = os.path.abspath(fname_namelist)
    namelist = twtnamelist.Namelist(filename=fname_namelist)

    kwargs = {'fname_domain' : namelist.fnames.domain,
              'verbose'      : namelist.options.verbose,
              'overwrite'    : namelist.options.overwrite}
    if namelist.options.domain_hucid is not None:
        kwargs.update({'domain_id'     : namelist.options.domain_hucid})
    elif namelist.options.domain_latlon is not None:
        kwargs.update({'domain_latlon' : namelist.options.domain_latlon})
    elif namelist.options.domain_bbox is not None:
        kwargs.update({'domain_bbox'   : namelist.options.domain_bbox})
    domain = twtdomain.set_domain(**kwargs)

    kwargs = {'dt_start'  : namelist.time.start_date,
              'dt_end'    : namelist.time.end_date,
              'savedir'   : namelist.dirnames.wtd_raw,
              'domain'    : domain,
              'verbose'   : namelist.options.verbose,
              'overwrite' : namelist.options.overwrite}
    hf_hydrodata.register_api_pin(namelist.options.hf_hydrodata_un, namelist.options.hf_hydrodata_pin)
    twtwt.download_hydroframe_data(**kwargs)

    kwargs = {'domain'    : domain,
              'dem_rez'   : namelist.options.dem_rez,
              'fname_dem' : namelist.fnames.dem,
              'verbose'   : namelist.options.verbose,
              'overwrite' : namelist.options.overwrite}
    twttopo.download_dem(**kwargs)

    kwargs = {'domain'             : domain,
              'fname_dem_breached' : namelist.fnames.dem_breached,
              'fname_dem'          : namelist.fnames.dem,
              'verbose'            : namelist.options.verbose,
              'overwrite'          : namelist.options.overwrite}
    twttopo.breach_dem(**kwargs)

    kwargs = {'fname_dem_breached' : namelist.fnames.dem_breached,
              'fname_facc_ncells'  : namelist.fnames.facc_ncells,
              'fname_facc_sca'     : namelist.fnames.facc_sca,
              'verbose'            : namelist.options.verbose,
              'overwrite'          : namelist.options.overwrite}
    twttopo.set_flow_acc(**kwargs)

    kwargs = {'fname_facc_ncells'     : namelist.fnames.facc_ncells,
              'facc_threshold_ncells' : namelist.options.facc_strm_thresh_ncells,
              'fname_strm_mask'       : namelist.fnames.stream_mask,
              'verbose'               : namelist.options.verbose,
              'overwrite'             : namelist.options.overwrite}
    twttopo.calc_stream_mask(**kwargs)

    kwargs = {'fname_dem_breached' : namelist.fnames.dem_breached,
              'fname_slope'        : namelist.fnames.slope,
              'verbose'            : namelist.options.verbose,
              'overwrite'          : namelist.options.overwrite}
    twttopo.calc_slope(**kwargs)

    kwargs = {'fname_facc_sca' : namelist.fnames.facc_sca,
              'fname_twi'      : namelist.fnames.twi,
              'fname_slope'    : namelist.fnames.slope,
              'verbose'        : namelist.options.verbose,
              'overwrite'      : namelist.options.overwrite}
    twttopo.calc_twi(**kwargs)

    kwargs = {'fname_twi_mean' : namelist.fnames.twi_mean,
              'fname_twi'      : namelist.fnames.twi,
              'wtd_raw_dir'    : namelist.dirnames.wtd_raw,
              'verbose'        : namelist.options.verbose,
              'overwrite'      : namelist.options.overwrite}
    twttopo.calc_twi_mean(**kwargs)

    kwargs = {'fname_texture'  : namelist.fnames.soil_texture,
              'domain'         : domain,
              'verbose'        : namelist.options.verbose,
              'overwrite'      : namelist.options.overwrite}
    await twtsoils.set_soil_texture(**kwargs)

    kwargs = {'fname_texture'        : namelist.fnames.soil_texture,
              'fname_transmissivity' : namelist.fnames.soil_transmissivity,
              'fname_dem'            : namelist.fnames.dem_breached,
              'verbose'              : namelist.options.verbose,
              'overwrite'            : namelist.options.overwrite}
    twtsoils.set_soil_transmissivity(**kwargs)

    kwargs = {'fname_streams' : namelist.fnames.nhdp,
              'domain'        : domain,
              'verbose'       : namelist.options.verbose,
              'overwrite'     : namelist.options.overwrite}
    twtstreams.set_streams(**kwargs)

    kwargs = {'dt_start'                  : namelist.time.start_date,
              'dt_end'                    : namelist.time.end_date,
              'wtd_raw_dir'               : namelist.dirnames.wtd_raw,
              'inundation_out_dir'        : namelist.dirnames.output_raw,
              'fname_soil_transmissivity' : namelist.fnames.soil_transmissivity,
              'fname_twi'                 : namelist.fnames.twi,
              'fname_twi_mean'            : namelist.fnames.twi_mean,
              'verbose'                   : namelist.options.verbose,
              'overwrite'                 : namelist.options.overwrite}
    twtcalc.calculate_inundation(**kwargs)

    kwargs = {'dt_start'                  : namelist.time.start_date,
              'dt_end'                    : namelist.time.end_date,
              'inundation_raw_dir'        : namelist.dirnames.output_raw,
              'inundation_summary_dir'    : namelist.dirnames.output_summary,
              'fname_dem'                 : namelist.fnames.dem_breached,
              'verbose'                   : namelist.options.verbose,
              'overwrite'                 : namelist.options.overwrite}
    fname_perc_inundated = twtcalc.calculate_summary_perc_inundated(**kwargs)

    kwargs = {'fname_perc_inundation'     : fname_perc_inundated,
              'fname_strm_mask'           : namelist.fnames.stream_mask,
              'fname_dem'                 : namelist.fnames.dem_breached,
              'verbose'                   : namelist.options.verbose,
              'overwrite'                 : namelist.options.overwrite}
    twtcalc.calculate_strm_permanence(**kwargs)