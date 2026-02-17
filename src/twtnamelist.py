import os,sys,rasterio,yaml,datetime,numpy

class Namelist:

    class DirectoryNames:
        project                 = None
        input                   = None
        output                  = None
        wtd_raw                 = None
        wtd_resampled           = None
        output_raw              = None
        output_summary          = None

    class Time:
        start_date              = None
        end_date                = None
        datetime_dim            = None

    class FileNames:
        domain                  = None
        twi                     = None
        twi_mean                = None
        soil_texture            = None
        soil_transmissivity     = None
        nhdp                    = None
        dem                     = None
        dem_breached            = None
        facc_ncells             = None
        facc_sca                = None
        stream_mask             = None
        slope                   = None

    class Options:
        domain_hucid            = None    
        domain_bbox             = None
        domain_latlon           = None
        overwrite               = False
        verbose                 = False
        resample_method         = None
        facc_strm_thresh_ncells = 1000
        facc_strm_thresh_sca    = None
        write_wtd_resampled     = False
        hf_hydrodata_un         = None
        hf_hydrodata_pin        = None
        dem_rez                 = None

    def __init__(self,filename:str):
        self._init_vars()
        self._set_user_inputs(filename)
        self._set_names()

    def _init_vars(self):
        self.dirnames = Namelist.DirectoryNames()
        self.fnames   = Namelist.FileNames()
        self.options  = Namelist.Options()
        self.time     = Namelist.Time()

    def _set_names(self):
        self._set_d_names()
        self._set_f_names()

    def _set_d_names(self):
        self.dirnames.input             = os.path.join(self.dirnames.project, 'input')
        self.dirnames.output            = os.path.join(self.dirnames.project, 'output')
        self.dirnames.wtd_raw           = os.path.join(self.dirnames.input,'wtd','raw')
        self.dirnames.wtd_resampled     = os.path.join(self.dirnames.input,'wtd','resampled')
        self.dirnames.output_raw        = os.path.join(self.dirnames.output,'raw')
        self.dirnames.output_summary    = os.path.join(self.dirnames.output,'summary')

    def _set_f_names(self):
        self.fnames.domain              = os.path.join(self.dirnames.input, 'domain.gpkg')
        self.fnames.dem                 = os.path.join(self.dirnames.input, 'dem.tiff')
        self.fnames.dem_breached        = os.path.join(self.dirnames.input, 'dem_breached.tiff')
        self.fnames.twi                 = os.path.join(self.dirnames.input, 'twi.tiff')
        self.fnames.twi_mean            = os.path.join(self.dirnames.input, 'twi_mean.tiff')
        self.fnames.soil_texture        = os.path.join(self.dirnames.input, 'soil_texture.gpkg')
        self.fnames.soil_transmissivity = os.path.join(self.dirnames.input, 'soil_transmissivity.tiff')
        self.fnames.facc_ncells         = os.path.join(self.dirnames.input, 'facc_ncells.tiff')
        self.fnames.facc_sca            = os.path.join(self.dirnames.input, 'facc_sca.tiff')
        self.fnames.stream_mask         = os.path.join(self.dirnames.input, 'stream_mask.tiff')
        self.fnames.slope               = os.path.join(self.dirnames.input, 'slope.tiff')

    def _read_inputyaml(self,fname:str):
        self.fnames.namlistyaml = fname
        with open(self.fnames.namlistyaml,'r') as yamlf:
            try: 
                return yaml.safe_load(yamlf)
            except yaml.YAMLError as yerr: 
                print(yerr)

    def _set_user_inputs(self,fname_yaml_input:str):
        """Set variables using read-in values"""
        #
        #
        userinput = self._read_inputyaml(fname_yaml_input)
        #
        #
        self.dirnames.project = os.path.dirname(os.path.abspath(fname_yaml_input))
        #print(f'project directory set to: {self.dirnames.project}')
        #
        #
        names_domain = ['domain_huc','domain_latlon','domain_bbox']
        if len([name for name in names_domain if name in userinput]) == 0:
            sys.exit(f'ERROR at least one of the required domain variables ({', '.join(names_domain)}) not found {fname_yaml_input}')
        name_var = 'domain_huc'
        if name_var in userinput:
            self.options.domain_hucid = userinput[name_var]
        name_var = 'domain_latlon'
        if name_var in userinput:
            self.options.domain_latlon = userinput[name_var]
        name_var = 'domain_bbox'
        if name_var in userinput:
            self.options.domain_bbox = userinput[name_var]
        #
        #
        name_var = 'start_date'
        if name_var not in userinput:
            sys.exit(f'ERROR required variable {name_var} not found {fname_yaml_input}')
        try:
            dt = userinput[name_var].split('-')
            dt = datetime.datetime(year=int(dt[0]),month=int(dt[1]),day=int(dt[2]))
            self.time.start_date = dt
        except ValueError:
            sys.exit(f'ERROR invalid start date {userinput[name_var]} in {fname_yaml_input}')
        #
        #
        name_var = 'end_date'
        if name_var not in userinput:
            sys.exit(f'ERROR required variable {name_var} not found {fname_yaml_input}')
        try:
            dt = userinput[name_var].split('-')
            dt = datetime.datetime(year=int(dt[0]),month=int(dt[1]),day=int(dt[2]))
            self.time.end_date = dt
        except ValueError:
            sys.exit(f'ERROR invalid start date {userinput[name_var]} in {fname_yaml_input}')
        #
        #
        dt_dim = list()
        idt = self.time.start_date
        while idt <= self.time.end_date:
            dt_dim.append(idt)
            idt += datetime.timedelta(days=1)
        self.time.datetime_dim = numpy.array(dt_dim)
        #
        #
        name_var = 'facc_strm_threshold_ncells'
        if name_var not in userinput:
            sys.exit(f'ERROR required variable {name_var} not found {fname_yaml_input}')
        try:
            self.options.facc_strm_thresh_ncells = int(userinput[name_var])
        except ValueError:
            sys.exit(f'ERROR invalid start date {userinput[name_var]} in {fname_yaml_input}')
        #
        #
        name_var = 'facc_strm_threshold_sca'
        if name_var not in userinput:
            sys.exit(f'ERROR required variable {name_var} not found {fname_yaml_input}')
        try:
            self.options.facc_strm_thresh_sca = float(userinput[name_var])
        except ValueError:
            sys.exit(f'ERROR invalid start date {userinput[name_var]} in {fname_yaml_input}')
        #
        #
        name_var = 'overwrite'
        if name_var in userinput and str(userinput[name_var]).upper().find('TRUE') != -1:
            self.options.overwrite = True
        else:
            self.options.overwrite = False
        #
        #
        name_var = 'verbose'
        if name_var in userinput and str(userinput[name_var]).upper().find('TRUE') != -1:
            self.options.verbose = True
        else:
            self.options.verbose = False
        #
        #
        name_var = 'wtd_resample_method'
        if name_var in userinput:
            if str(userinput[name_var]).lower().find('bilinear') != -1:
                self.options.resample_method = rasterio.enums.Resampling.bilinear
            elif str(userinput[name_var]).lower().find('cubic') != -1:
                self.options.resample_method = rasterio.enums.Resampling.cubic
            elif str(userinput[name_var]).lower().find('nearest') != -1:
                self.options.resample_method = rasterio.enums.Resampling.nearest
            else:
                sys.exit(f'ERROR invalid wtd resample method {userinput[name_var]} in {fname_yaml_input}')
        else:
            self.options.resample_method = rasterio.enums.Resampling.bilinear
        #
        #
        name_var = 'hf_hydrodata_un'
        if name_var in userinput:
            try:
                self.options.hf_hydrodata_un = userinput[name_var]
            except ValueError:
                sys.exit(f'ERROR invalid {name_var} {userinput[name_var]} in {fname_yaml_input}')
        #
        #
        name_var = 'hf_hydrodata_pin'
        if name_var in userinput:
            try:
                self.options.hf_hydrodata_pin = userinput[name_var]
            except ValueError:
                sys.exit(f'ERROR invalid {name_var} {userinput[name_var]} in {fname_yaml_input}')
        #
        #
        name_var = 'write_wtd_resampled'
        if name_var in userinput and str(userinput[name_var]).upper().find('TRUE') != -1:
            self.options.write_wtd_resampled = True
        else:
            self.options.write_wtd_resampled = False
        #
        #
        name_var = 'dem_rez'
        if name_var in userinput:
            try:
                self.options.dem_rez = float(userinput[name_var])
            except ValueError:
                sys.exit(f'ERROR invalid {name_var} {userinput[name_var]} in {fname_yaml_input}')