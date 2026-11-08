from pathlib import Path
import csv,json,collections,hashlib,os

root=Path.cwd()
file=next(root.glob('DATASET*.csv'))
years=collections.Counter(); local_years=collections.Counter(); modes=collections.Counter(); local_modes=collections.Counter(); district=collections.Counter(); street=collections.Counter(); ymodes=collections.Counter()
depts=set(); ubigeos=set(); months=set(); count=valid_rows=local_rows=other_rows=outside_rows=0; minimum=10**9; maximum=0
with file.open(encoding='utf-8-sig',newline='') as f:
    reader=csv.reader(f)
    header=next(reader)
    for r in reader:
        count+=1
        assert len(r)==8
        y,m=int(r[0]),int(r[1]); qty=int(r[7]); mode=r[6]
        assert 2018<=y<=2026 and 1<=m<=12 and 0<qty<=100000
        years[y]+=qty; modes[mode]+=qty; ymodes[(y,mode)]+=qty
        depts.add(r[2]); ubigeos.add(r[5].zfill(6)); months.add((y,m)); minimum=min(minimum,qty); maximum=max(maximum,qty)
        local=r[2]=='LIMA METROPOLITANA' or 'CALLAO' in r[2] or (r[2]=='LIMA' and r[3]=='LIMA')
        if not local:
            outside_rows+=1
            continue
        local_rows+=1; local_years[y]+=qty
        if mode=='Otros':
            other_rows+=1
            continue
        valid_rows+=1; local_modes[mode]+=qty; district[r[4]]+=qty
        if mode in ['Hurto','Robo','Extorsión','Secuestro']:
            street[r[4]]+=qty

result={'archivo':file.name,'cabecera':header,'filas':count,'nacional':sum(years.values()),'anual':dict(years),'lima_callao_anual':dict(local_years),'modalidades':dict(modes),'clases_en_alcance':dict(local_modes),'ubigeos':len(ubigeos),'departamentos':len(depts),'meses':len(months),'cantidad_min':minimum,'cantidad_max':maximum,'filas_fuera_alcance':outside_rows,'filas_en_alcance':local_rows,'filas_otros_en_alcance':other_rows,'filas_clasificables':valid_rows,'registros_clasificables':sum(local_modes.values()),'distritos_clasificables':len(district),'top10_total':sum(v for _,v in district.most_common(10)),'top10_calle':sum(v for _,v in street.most_common(10)),'total_calle':sum(street.values()),'extorsion_2018':ymodes[(2018,'Extorsión')],'extorsion_2025':ymodes[(2025,'Extorsión')],'estafa_2018':ymodes[(2018,'Estafa')],'estafa_2024':ymodes[(2024,'Estafa')],'sha256':hashlib.sha256(file.read_bytes()).hexdigest()}
dest=Path(__file__).with_name('csv-verificado.json')
dest.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(result,ensure_ascii=False,indent=2))
