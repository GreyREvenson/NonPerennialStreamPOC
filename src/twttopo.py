import os
import rasterio
import py3dep
import geopandas
import rioxarray
from geocube.api.core import make_geocube
from whitebox_workflows import WbEnvironment

def download_dem(**kwargs):
    domain        = kwargs.get('domain',    None)
    verbose       = kwargs.get('verbose',   False)
    overwrite     = kwargs.get('overwrite', False)
    dem_rez       = kwargs.get('dem_rez',   None)
    fname_dem     = kwargs.get('fname_dem', None)
    if verbose: print('download_dem')
    if not isinstance(domain,geopandas.GeoDataFrame):
        raise TypeError(f'download_dem domain argument is missing or is not type geopandas.geodataframe')
    if fname_dem is None:
        raise ValueError(f'download_dem missing required argument fname_dem')
    if not os.path.isfile(fname_dem) or overwrite:
        avail = py3dep.check_3dep_availability(bbox=tuple(domain.total_bounds),crs=domain.crs)
        vals  = list()
        for k in avail.keys():
            try: vals.append(int(k.replace('m','')))
            except: pass
        if len(vals) == 0:
            raise ValueError(f'download_dem 3dep could not find dem resolution')
        rez = min(vals)
        if dem_rez in vals: rez = dem_rez # override with user input if available
        if verbose:
            print(f' downloading dem ({str(rez)}m) using py3dep. saving to: {fname_dem}')
        dem = py3dep.get_dem(geometry   = domain.geometry.union_all(),
                             resolution = rez,
                             crs        = domain.crs)
        dem.rio.to_raster(fname_dem)
    else:
        if verbose: print(f' using existing dem {fname_dem}')

def set_domain_mask(**kwargs):
    domain            = kwargs.get('domain',            None)
    verbose           = kwargs.get('verbose',           False)
    overwrite         = kwargs.get('overwrite',         False)
    fname_domain_mask = kwargs.get('fname_domain_mask', None)
    fname_dem         = kwargs.get('fname_dem',         None)
    if verbose: print(f'set_domain_mask')
    if not os.path.isfile(fname_dem):
        raise ValueError(f'set_domain_mask could not find fname_dem {fname_dem}')
    if not os.path.isfile(fname_domain_mask) or overwrite:
        if verbose: print(f' creating {fname_domain_mask}')
        with rioxarray.open_rasterio(fname_dem) as riox_ds_dem:
            domain = domain.to_crs(riox_ds_dem.rio.crs)
            domain = domain.dissolve()
            domain = domain.drop(columns=[col for col in domain.columns if col not in ['geometry']])
            domain['mask'] = 1
            domain_mask = make_geocube(vector_data=domain,like=riox_ds_dem,measurements=['mask'])
            domain_mask.rio.to_raster(fname_domain_mask)
    else:
        if verbose: print(f' using existing domain mask {fname_domain_mask}')

def breach_dem(**kwargs):
    verbose            = kwargs.get('verbose',            False)
    overwrite          = kwargs.get('overwrite',          False)
    fname_dem_breached = kwargs.get('fname_dem_breached', None)
    fname_dem          = kwargs.get('fname_dem',          None)
    if verbose: print('breach_dem')
    if fname_dem is None:
        raise KeyError('breach_dem missing required argument fname_dem')
    if not os.path.isfile(fname_dem):
        raise ValueError(f'breach_dem could not find fname_dem {fname_dem}')
    if not os.path.isfile(fname_dem_breached) or overwrite:
        if verbose: print(f' using whitebox to breach dem and writing to {fname_dem_breached}')
        wbe = WbEnvironment()
        if verbose: wbe.verbose = True
        wbe.working_directory = os.path.dirname(fname_dem)
        dem = wbe.read_raster(fname_dem)
        dem_breached = wbe.breach_depressions_least_cost(dem=dem,
                                                         fill_deps=False,
                                                         max_dist=1000)
        wbe.write_raster(dem_breached, fname_dem_breached, compress=True)
    else:
        if verbose: print(f' using existing breached dem {fname_dem_breached}')

def set_flow_acc(**kwargs):
    verbose            = kwargs.get('verbose',            False)
    overwrite          = kwargs.get('overwrite',          False)
    fname_dem_breached = kwargs.get('fname_dem_breached', None)
    fname_facc_ncells  = kwargs.get('fname_facc_ncells',  None)
    fname_facc_sca     = kwargs.get('fname_facc_sca',     None)
    if verbose: print(f'set_flow_acc')
    if fname_dem_breached is None:
        raise KeyError(f'set_flow_acc missing required argument set_flow_acc')
    if not os.path.isfile(fname_dem_breached):
        raise ValueError(f'set_flow_acc could not find fname_dem_breached {fname_dem_breached}')
    if fname_facc_ncells is None and fname_facc_sca is None:
        raise KeyError(f'set_flow_acc missing required argument fname_facc_ncells or fname_facc_sca')
    if not os.path.isfile(fname_facc_ncells) or overwrite:
        if verbose: print(f' using whitebox to calculate flow accumulation (n cells), writing to {fname_facc_ncells}')
        wbe = WbEnvironment()
        if verbose: wbe.verbose = True
        wbe.working_directory = os.path.dirname(fname_dem_breached)
        facc_ncells = wbe.dinf_flow_accum(dem=wbe.read_raster(fname_dem_breached),
                                          out_type='cells')
        wbe.write_raster(facc_ncells, fname_facc_ncells, compress=True)
    else:
        if verbose: print(f' using existing flow accumulation (ncells) file {fname_facc_ncells}')
    if not os.path.isfile(fname_facc_sca) or overwrite:
        if verbose: print(f' using whitebox to calculate flow accumulation (sca), writing to {fname_facc_sca}')
        wbe = WbEnvironment()
        if verbose: wbe.verbose = True
        wbe.working_directory = os.path.dirname(fname_dem_breached)
        facc_sca = wbe.dinf_flow_accum(dem=wbe.read_raster(fname_dem_breached),
                                       out_type='sca')
        wbe.write_raster(facc_sca, fname_facc_sca, compress=True)
    else:
        if verbose: print(f' using existing flow accumulation (sca) file {fname_facc_sca}')

def calc_stream_mask(**kwargs):
    ##TODO fnames might not be os.path.isfile()
    verbose               = kwargs.get('verbose',              False)
    overwrite             = kwargs.get('overwrite',            False)
    fname_facc_ncells     = kwargs.get('fname_facc_ncells',    None)
    fname_facc_sca        = kwargs.get('fname_facc_sca',       None)
    facc_threshold_ncells = kwargs.get('facc_threshold_ncells',None)
    facc_threshold_sca    = kwargs.get('facc_threshold_sca',   None)
    fname_strm_mask       = kwargs.get('fname_strm_mask',      None)
    if verbose: print('calling calc_stream_mask')
    if fname_facc_ncells is None and fname_facc_sca is None:
        raise KeyError(f'calc_stream_mask missing required argument fname_facc_ncells or fname_facc_sca')
    if facc_threshold_ncells is None and facc_threshold_sca is None:
        raise KeyError(f'calc_stream_mask missing required argument facc_threshold_ncells or facc_threshold_sca') 
    if fname_facc_ncells is None and facc_threshold_ncells is not None:
        raise KeyError(f'calc_stream_mask missing required argument fname_facc_ncells while given facc_threshold_ncells')
    if fname_facc_ncells is not None and facc_threshold_ncells is None:
        raise KeyError(f'calc_stream_mask missing required argument facc_threshold_ncells while given fname_facc_ncells')
    if fname_facc_sca is None and facc_threshold_sca is not None:
        raise KeyError(f'calc_stream_mask missing required argument fname_facc_sca while given facc_threshold_sca')
    if fname_facc_sca is not None and facc_threshold_sca is None:
        raise KeyError(f'calc_stream_mask missing required argument facc_threshold_sca while given fname_facc_sca')
    if not os.path.isfile(fname_strm_mask) or overwrite:
        if os.path.isfile(fname_facc_ncells):
            if verbose: print(f' setting stream mask using fname_facc_ncells {fname_facc_ncells} and facc_threshold_ncells {facc_threshold_ncells}')
            wbe = WbEnvironment()
            if verbose: wbe.verbose = True
            wbe.working_directory = os.path.dirname(fname_facc_ncells)
            strm_mask_facc_ncells = wbe.extract_streams(flow_accumulation=wbe.read_raster(fname_facc_ncells),
                                                        threshold=facc_threshold_ncells,
                                                        zero_background=True)
            wbe.write_raster(strm_mask_facc_ncells,fname_strm_mask,compress=True)
        elif os.path.isfile(fname_facc_sca):
            if verbose: print(f' setting stream mask using fname_facc_sca {fname_facc_sca} and facc_threshold_sca {facc_threshold_sca}')
            wbe = WbEnvironment()
            if verbose: wbe.verbose = True
            wbe.working_directory = os.path.dirname(fname_facc_sca)
            strm_mask_facc_sca = wbe.extract_streams(flow_accumulation=wbe.read_raster(fname_facc_sca),
                                                        threshold=facc_threshold_sca,
                                                        zero_background=True)
            wbe.write_raster(strm_mask_facc_sca,fname_strm_mask,compress=True)
        else:
            raise Exception(f'calc_stream_mask did not find valid flow accumulation file fname_facc_ncells {fname_facc_ncells} or fname_facc_sca {fname_facc_sca}')
    else:
        if verbose: print(f' using existing stream mask {fname_strm_mask}')

def calc_slope(**kwargs):
    fname_dem_breached = kwargs.get('fname_dem_breached', None)
    fname_slope        = kwargs.get('fname_slope',        None)
    verbose            = kwargs.get('verbose',            False)
    overwrite          = kwargs.get('overwrite',          False)
    if verbose: print(f'calling calc_slope')
    if fname_dem_breached is None or not os.path.isfile(fname_dem_breached):
        raise ValueError(f'calc_slope missing required argument fname_dem_breached is missing or invalid file {fname_dem_breached}')
    if not os.path.isfile(fname_slope) or overwrite:
        if verbose: print(f' using whitebox workflows to calculate {fname_slope} from {fname_dem_breached}')
        wbe = WbEnvironment()
        if verbose: wbe.verbose = True
        wbe.working_directory = os.path.dirname(fname_dem_breached)
        slope = wbe.slope(dem = wbe.read_raster(fname_dem_breached),
                          units = 'degrees')
        wbe.write_raster(slope,fname_slope,compress=True)
    else:
        if verbose: print(f' using existing slope file {fname_slope}')

def calc_twi(**kwargs):
    fname_facc_sca     = kwargs.get('fname_facc_sca', None)
    fname_slope        = kwargs.get('fname_slope',    None)
    fname_twi          = kwargs.get('fname_twi',      None)
    verbose            = kwargs.get('verbose',        False)
    overwrite          = kwargs.get('overwrite',      False)
    if verbose: print('calling calc_twi')
    if fname_facc_sca is None or not os.path.isfile(fname_facc_sca):
        raise ValueError(f'calc_twi missing required argument fname_facc_sca or the argument {fname_facc_sca} is not a valid file')        
    if fname_slope is None or not os.path.isfile(fname_slope):
        raise ValueError(f'calc_twi missing required argument fname_slope or the argument {fname_slope} is not a valid file')        
    if fname_twi is None:
        raise KeyError(f'calc_twi missing required argument fname_twi')
    if not os.path.isfile(fname_twi) or overwrite:
        if verbose: print(f' using whitebox workflows to calculate {fname_twi}')
        wbe = WbEnvironment()
        if verbose: wbe.verbose = True
        wbe.working_directory = os.path.dirname(fname_facc_sca)
        twi = wbe.wetness_index(specific_catchment_area = wbe.read_raster(fname_facc_sca),
                                slope = wbe.read_raster(fname_slope))
        wbe.write_raster(twi,fname_twi,compress=True)
    else:
        if verbose: print(f' found existing TWI file {fname_twi}')

def calc_twi_mean(**kwargs):
    fname_twi          = kwargs.get('fname_twi',      None)
    fname_twi_mean     = kwargs.get('fname_twi_mean', None)
    wtd_raw_dir        = kwargs.get('wtd_raw_dir',    None)
    verbose            = kwargs.get('verbose',        False)
    overwrite          = kwargs.get('overwrite',      False)
    if verbose: print('calling calc_twi_mean')
    if fname_twi is None or not os.path.isfile(fname_twi):
        raise ValueError(f'calc_calc_twi_meanmean_twi missing required argument fname_twi or argument {fname_twi} is not valid file')
    if wtd_raw_dir is None or not os.path.isdir(wtd_raw_dir):
        raise ValueError(f'calc_calc_twi_meanmean_twi missing required argument wtd_raw_dir or argument {wtd_raw_dir} is not valid file')
    if not os.path.isfile(fname_twi_mean) or overwrite:
        fname_example_wtd_raw = [fn for fn in os.listdir(wtd_raw_dir) if str(fn).endswith('.tiff')][0]
        fname_example_wtd_raw = os.path.join(wtd_raw_dir,fname_example_wtd_raw)
        if not os.path.isfile(fname_example_wtd_raw):
            raise ValueError(f'calc_twi_mean could not locate example raw wtd file - {fname_example_wtd_raw} is not valid file')
        if verbose: print(f' calculating mean twi and saving to {fname_twi_mean} (calculating mean twi in each wtd grid cell)')
        with rioxarray.open_rasterio(fname_twi,masked=True) as riox_ds_twi, rioxarray.open_rasterio(fname_example_wtd_raw,masked=True) as riox_ds_wtd:
            twi_mean = riox_ds_twi.rio.reproject_match(riox_ds_wtd, resampling=rasterio.enums.Resampling.average)
            twi_mean = twi_mean.rio.reproject_match(riox_ds_twi, resample=rasterio.enums.Resampling.nearest)
            twi_mean.rio.to_raster(fname_twi_mean,compress=True)
    else:
        if verbose: print(f' found existing mean TWI file {fname_twi_mean}')







