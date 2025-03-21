import os,sys,rasterio

class Namelist:

    class DirectoryNames:
        """Directory names"""
        project                 = ''
        data                    = ''
        wtd                     = ''
        wtd_parflow_raw         = ''
        wtd_parflow_bilinear    = ''
        wtd_parflow_nearest     = ''
        wtd_parflow_cubic       = ''
        wtd_fan                 = ''
        dem                     = ''
        twi                     = ''
        soils                   = ''
        domain                  = ''
        nhd                     = '' 
        pysda                   = ''
        output                  = ''

    class FileNames:
        """File names including full path"""
        namelist                = ''
        hucs                    = ''
        domain                  = ''
        domain_mask             = ''
        nhd                     = ''
        dem                     = ''
        dem_original            = ''
        dem_breached            = ''
        soil_texture            = ''
        soil_transmissivity     = ''
        flow_acc                = ''
        slope                   = ''
        twi                     = ''
        twi_upsample            = ''
        twi_downsample          = ''

    class Variables:
        """Variables"""
        hucs                    = ''
        huc_level               = ''
        start_date              = ''
        end_date                = ''

    class Options:
        """Options"""
        overwrite_flag          = False
        verbose                 = False

    class ParflowSpatialInformation:
        """Parflow grid info - see https://hf-hydrodata.readthedocs.io/en/latest/available_grids.html"""
        conus1_proj      = '+proj=lcc +lat_1=33 +lat_2=45 +lon_0=-96.0 +lat_0=39 +a=6378137.0 +b=6356752.31'
        conus1_spatext   = tuple([-121.47939483437318, 31.651836025255015, -76.09875469594509, 50.49802132270979])
        conus1_transform = rasterio.transform.Affine(1000.0,0.0,-1885055.4995,0.0,1000.0,-604957.0654)
        conus1_shape     = (1888,3342)
        conus2_proj      = '+proj=lcc +lat_1=30 +lat_2=60 +lon_0=-97.0 +lat_0=40.0000076294444 +a=6370000.0 +b=6370000'
        conus2_spatext   = tuple([-126.88755692881833, 21.8170599154073, -64.7677149695924, 53.20274381640737])
        conus2_transform = rasterio.transform.Affine(1000.0,0.0,-2208000.30881173,0.0,1000.0,-1668999.65483222)
        conus2_shape     = (3256,4442)

    def __init__(self,filename:str):
        """Initialize namelist"""
        self._init_vars()
        self._read_namelist(filename)
        self._set_user_inputs()
        self._set_names()

    def _init_vars(self):
        """Initialize variables"""
        self.dirnames = Namelist.DirectoryNames()
        self.fnames   = Namelist.FileNames()
        self.vars     = Namelist.Variables()
        self.options  = Namelist.Options()
        self.parflow  = Namelist.ParflowSpatialInformation()
        
    def _set_names(self):
        """Set static directory and file names"""
        self._set_subdirectory_names()
        self._set_file_names()

    def _set_subdirectory_names(self):
        """Set project subdirectory names"""
        self.dirnames.data                    = os.path.join(self.dirnames.project,'data')
        self.dirnames.wtd                     = os.path.join(self.dirnames.project,'wtd')
        self.dirnames.wtd_parflow_raw         = os.path.join(self.dirnames.wtd,'parflow_raw')
        self.dirnames.wtd_parflow_bilinear    = os.path.join(self.dirnames.wtd,'parflow_bilinear')
        self.dirnames.wtd_parflow_nearest     = os.path.join(self.dirnames.wtd,'parflow_nearest')
        self.dirnames.wtd_parflow_cubic       = os.path.join(self.dirnames.wtd,'parflow_cubic')
        self.dirnames.wtd_fan                 = os.path.join(self.dirnames.wtd,'fan')
        self.dirnames.dem                     = os.path.join(self.dirnames.project,'dem')
        self.dirnames.twi                     = os.path.join(self.dirnames.project,'twi')
        self.dirnames.soils                   = os.path.join(self.dirnames.project,'soils')
        self.dirnames.domain                  = os.path.join(self.dirnames.project,'domain')
        self.dirnames.nhd                     = os.path.join(self.dirnames.project,'nhd')
        self.dirnames.output                  = os.path.join(self.dirnames.project,'output')

    def _set_file_names(self):
        """Set static files names for intermediate output files"""
        self.fnames.domain              = os.path.join(self.dirnames.domain,'domain.gpkg')
        self.fnames.domain_mask         = os.path.join(self.dirnames.domain,'domain_mask.gpkg')
        self.fnames.hucs                = os.path.join(self.dirnames.domain,'hucs.gpkg')
        self.fnames.nhd                 = os.path.join(self.dirnames.nhd,'nhdhr.gpkg')
        self.fnames.dem                 = os.path.join(self.dirnames.dem,'dem.tif')
        self.fnames.dem_original        = os.path.join(self.dirnames.dem,'dem_original.tif')
        self.fnames.dem_breached        = os.path.join(self.dirnames.dem,'dem_breached.tif')
        self.fnames.soil_texture        = os.path.join(self.dirnames.soils,'soiltexture.gpkg')
        self.fnames.soil_transmissivity = os.path.join(self.dirnames.soils,'soiltransmissivity.gpkg')
        self.fnames.flow_acc            = os.path.join(self.dirnames.dem,'flow_acc.tif')
        self.fnames.slope               = os.path.join(self.dirnames.dem,'slope.tif')
        self.fnames.twi                 = os.path.join(self.dirnames.twi,'twi.tif')
        self.fnames.twi_upsample        = os.path.join(self.dirnames.twi,'twi_upsample.tif')
        self.fnames.twi_downsample      = os.path.join(self.dirnames.twi,'twi_downsample.tif')

    def _remove_whitespace_outside_quotes(self,line:str):
        result = []
        in_quote = False
        quote_char = None
        for char in line:
            if char in ('"', "'"):
                if in_quote and quote_char == char:
                    in_quote = False
                else:
                    in_quote = True
                    quote_char = char
                result.append(char)
            elif not in_quote and char.isspace():
                continue
            else:
                result.append(char)
        return ''.join(result)

    def _read_namelist(self,filename:str):
        """Read namelist file into generic dictionary"""
        self.fnames.namelist = filename
        if not os.path.isfile(self.fnames.namelist):
            sys.exit('ERROR could not find namelist file '+self.fnames.namelist)
        self.vars.file_inputs = dict()
        namelist_lines = list(open(self.fnames.namelist,'r'))
        for l in namelist_lines:
            try:
                l0 = self._remove_whitespace_outside_quotes(line=l)
                if len(l0) > 0:
                    if str(l[0:1]).find('#') == -1:
                        l1 = l0.split('=')
                        var_name = str(l1[0])
                        var_vals = str(l1[1])
                        var_vals = var_vals.split(',')
                        for i in range(len(var_vals)):
                            val = var_vals[i]
                            if val.startswith("'") and val.endswith("'"):
                                val = val[1:len(val)-1]
                            elif val.startswith('"') and val.endswith('"'):
                                val = val[1:len(val)-1]
                            var_vals[i] = val
                        self.vars.file_inputs[var_name] = var_vals
            except:
                sys.exit('ERROR could not read namelist.txt line: '+l)
        for var_name in self.vars.file_inputs:
            if isinstance(self.vars.file_inputs[var_name],list) and len(self.vars.file_inputs[var_name]) == 1:
                self.vars.file_inputs[var_name] = self.vars.file_inputs[var_name][0]

    def _set_user_inputs(self):
        """Set variables using read-in values"""
        name_project_dir      = 'project_directory'
        name_hucs             = 'hucs'
        name_overwrite        = 'overwrite'
        name_verbose          = 'verbose'
        name_pysda            = 'pysda'
        name_dem              = 'dem'
        name_start_date       = 'start_date'
        name_end_date         = 'end_date'
        req = [name_project_dir,name_hucs,name_pysda,name_start_date,name_end_date]
        for name in req:
            if name not in self.vars.file_inputs: sys.exit('ERROR required variable '+name+' not found in namelist file')
        self.dirnames.project = os.path.abspath(self.vars.file_inputs[name_project_dir])
        self.dirnames.pysda = os.path.abspath(self.vars.file_inputs[name_pysda])
        self.vars.start_date = self.vars.file_inputs[name_start_date]
        self.vars.end_date = self.vars.file_inputs[name_end_date]
        if name_dem in self.vars.file_inputs:
            self.fnames.dem = os.path.abspath(self.vars.file_inputs[name_dem])
        self.vars.hucs = self.vars.file_inputs[name_hucs]
        if isinstance(self.vars.hucs,str):
            self.vars.huc_level = len(self.vars.file_inputs[name_hucs])
            if self.vars.huc_level not in [2,4,6,8,10,12]: sys.exit('ERROR invalid huc level in namelist file')
        elif isinstance(self.vars.hucs,list):
            self.vars.hucs = self.vars
            levs = set([len(self.vars.hucs[i]) for i in range(len(self.vars.hucs))])
            if len(levs) != 1 or self.vars.huc_level not in [2,4,6,8,10,12]: sys.exit('ERROR invalid huc level in namelist file')
            self.vars.huc_level = levs[0]
        if name_overwrite in self.vars.file_inputs and self.vars.file_inputs[name_overwrite].upper().find('TRUE') != -1:
            self.vars.overwrite_flag = True
        if name_verbose in self.vars.file_inputs and self.vars.file_inputs[name_verbose].upper().find('TRUE') != -1:
            self.vars.verbose = True