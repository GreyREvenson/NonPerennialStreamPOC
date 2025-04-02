import os,sys,numpy,rasterio,twtnamelist

def calc_parflow_inundation(namelist:twtnamelist.Namelist):
    """Calculate inundation using ParFlow simulations"""
    if namelist.options.verbose: print('calling calc_parflow_inundation')
    _calc_parflow_inundation_main(namelist)
    _calc_parflow_inundation_summary(namelist)

def _calc_parflow_inundation_summary(namelist:twtnamelist.Namelist):
    """Calculate summary grids"""
    if namelist.options.verbose: print('calling _calc_parflow_inundation_summary')
    _calc_parflow_inundation_summary_perc_inundated(namelist)
    _calc_parflow_inundation_summary_zerowtd(namelist)

def _calc_parflow_inundation_time_i(wtd_mean,twi_local,twi_mean,f,domain_mask):
    """Calculate inundation using 
    TOPMODEL-based equation - see Equation 3 of Zhang et al. (2016) (https://doi.org/10.5194/bg-13-1387-2016)
    Arguments:
        wtd_mean:    grid of mean water table depth values  (zeta sub m in equation 3 of Zhang et al.) 
        twi_local:   grid of local TWI values               (lambda sub l in equation 3 of Zhang et al.)
        twi_mean:    grid of mean TWI values                (lamda sub m in equation 3 of Zhang et al.)
        f:           grid of f parameter values             (f in equation 3 and table 1 of Zhang et al.)
        domain_mask: grid of model domain                   (1=domain,0=not domain)
    Returns:
        wtd_local:   grid of local water table depth values (zeta sub l in equation 3 of Zhang et al.)
    """
    wtd_mean  = wtd_mean*(-1)                                                             # values must be negative so multiply by -1
    wtd_local = numpy.where(domain_mask==1,(1/f)*(twi_local-twi_mean)+wtd_mean,numpy.nan) # calculate local water depth using equation 3 from Zhang et al. (2016) where domain_mask=1 (i.e., within the model domain), otherise give a NaN value 
    wtd_local = numpy.where(wtd_local>=0,1,numpy.nan)                                     # give value of 1 where local water table depth is >= 0 (i.e. at or above the surface), otherwise give a NaN value
    return wtd_local

def _calc_parflow_inundation_main(namelist:twtnamelist.Namelist):
    """Calculate inundated area"""
    if namelist.options.verbose: print('calling _calc_parflow_inundation_main')
    for fname in [namelist.fnames.twi,
                  namelist.fnames.twi_downsample,
                  namelist.fnames.soil_transmissivity,
                  namelist.fnames.domain_mask]:
        if not os.path.isfile(fname): sys.exit('ERROR could not find '+fname)
    twi_local                   = rasterio.open(namelist.fnames.twi,'r').read(1)
    twi_mean                    = rasterio.open(namelist.fnames.twi_downsample,'r').read(1)
    domain_mask                 = rasterio.open(namelist.fnames.domain_mask,'r').read(1)
    transmissivity_decay_factor = rasterio.open(namelist.fnames.soil_transmissivity,'r').read(1)
    for method in ['bilinear','nearest','cubic']:
        if method == 'bilinear': 
            wtd_dir = namelist.dirnames.wtd_parflow_bilinear
            out_dir = namelist.dirnames.output_raw_bilinear
        elif method == 'nearest': 
            wtd_dir = namelist.dirnames.wtd_parflow_nearest
            out_dir = namelist.dirnames.output_raw_neareast
        elif method == 'cubic': 
            wtd_dir = namelist.dirnames.wtd_parflow_cubic
            out_dir = namelist.dirnames.output_raw_cubic
        for idatetime in namelist.time.datetime_dim:
            fname_wtd_mean = os.path.join(wtd_dir,'wtd_'+idatetime.strftime('%Y%m%d')+'.tiff')
            fname_output = os.path.join(out_dir,'inundatedarea_'+idatetime.strftime('%Y%m%d')+'.tiff')
            if os.path.isfile(fname_wtd_mean):
                if not os.path.isfile(fname_output) or namelist.options.overwrite_flag:
                    with rasterio.open(fname_wtd_mean,'r') as wtd_mean_dataset:
                        wtd_mean = wtd_mean_dataset.read(1)
                        wtd_local = _calc_parflow_inundation_time_i(wtd_mean,twi_local,twi_mean,transmissivity_decay_factor,domain_mask)
                        with rasterio.open(fname_output, "w", **wtd_mean_dataset.meta) as wtd_local_dataset:
                            wtd_local_dataset.write(wtd_local,1)

def _calc_parflow_inundation_summary_perc_inundated(namelist:twtnamelist.Namelist):
    """Calculate summary grid of inundated area"""
    if namelist.options.verbose: print('calling _calc_parflow_inundation_summary_perc_inundated')
    domain_mask = rasterio.open(namelist.fnames.domain_mask,'r').read(1)
    for method in ['bilinear','nearest','cubic']:
        if method == 'bilinear': 
            out_dir = namelist.dirnames.output_summary_bilinear
            raw_dir = namelist.dirnames.output_raw_bilinear
        elif method == 'nearest': 
            out_dir = namelist.dirnames.output_summary_nearest
            raw_dir = namelist.dirnames.output_raw_neareast
        elif method == 'cubic':
            out_dir = namelist.dirnames.output_summary_cubic
            raw_dir = namelist.dirnames.output_raw_cubic
        fname_output = ['percent_inundated_grid_',
                        namelist.time.datetime_dim[0].strftime('%Y%m%d'),
                        '_to_',
                        namelist.time.datetime_dim[len(namelist.time.datetime_dim)-1].strftime('%Y%m%d'),
                        '.tiff']
        fname_output = os.path.join(out_dir,"".join(fname_output))
        if not os.path.isfile(fname_output) or namelist.options.overwrite_flag:
            count   = 0
            sumgrid = numpy.where(domain_mask==1,0,numpy.nan)
            total   = int((namelist.time.datetime_dim[len(namelist.time.datetime_dim)-1] - namelist.time.datetime_dim[0]).days)+1
            for idatetime in namelist.time.datetime_dim:
                fname_wtd_local = os.path.join(raw_dir,'inundatedarea_'+idatetime.strftime('%Y%m%d')+'.tiff') 
                if os.path.isfile(fname_wtd_local):
                    inundation_data = rasterio.open(fname_wtd_local,'r').read(1)
                    sumgrid += numpy.where(inundation_data==1,1,0)
                    count += 1
                else:
                    break
            if count > 0:
                if count != total: sys.exit('ERROR _calc_parflow_inundation_summary_perc_inundated count='+str(count)+' total='+str(total))
                perc_inundated = (sumgrid/float(total))/100.
                perc_inundated = numpy.where(domain_mask==1,perc_inundated,numpy.nan)
                with rasterio.open(fname_output, "w", **rasterio.open(namelist._get_dummy_hrwtd_fname(),'r').meta) as output_dataset:
                    output_dataset.write(perc_inundated,1)

def _calc_parflow_inundation_summary_zerowtd(namelist:twtnamelist.Namelist):
    """Calculate zero water table depth"""
    if namelist.options.verbose: print('calling _calc_parflow_inundation_summary_zerowtd')
    for method in ['bilinear','nearest','cubic']:
        if method == 'bilinear': out_dir = namelist.dirnames.output_summary_bilinear
        elif method == 'nearest': out_dir = namelist.dirnames.output_summary_nearest
        elif method == 'cubic': out_dir = namelist.dirnames.output_summary_cubic
        fname_output = os.path.join(out_dir,'mean_wtd_if_local_wtd_equals_0.tiff')
        if not os.path.isfile(fname_output) or namelist.options.overwrite_flag:
            domain_mask        = rasterio.open(namelist.fnames.domain_mask,'r').read(1)
            twi_local          = rasterio.open(namelist.fnames.twi,'r').read(1)
            twi_local          = numpy.where(domain_mask==1,twi_local,numpy.nan)
            twi_mean           = rasterio.open(namelist.fnames.twi_downsample,'r').read(1)
            twi_mean           = numpy.where(domain_mask==1,twi_mean,numpy.nan)
            trans_decay_factor = rasterio.open(namelist.fnames.soil_transmissivity,'r').read(1)
            trans_decay_factor = numpy.where(domain_mask==1,trans_decay_factor,numpy.nan)
            with rasterio.open(namelist._get_dummy_hrwtd_fname(),'r') as wtd_mean_t0:
                wtd_local = wtd_mean_t0.read(1)
                wtd_local = numpy.where(domain_mask==1,0,numpy.nan)
                wtd_mean  = numpy.where(domain_mask==1,((1/trans_decay_factor)*(twi_local-twi_mean)-wtd_local)*(-1),numpy.nan)
                with rasterio.open(fname_output, "w", **wtd_mean_t0.meta) as wtd_mean_dataset_wtd_local_0:
                    wtd_mean_dataset_wtd_local_0.write(wtd_mean,1)