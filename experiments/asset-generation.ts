/**
 * Asset generators for DELTA evaluation experiments
 */

enum AssetType {
    Simple = 'simple',
    Intermediate = 'intermediate',
    Complex = 'complex'
}

interface SimpleAsset {
    value1: number,
    value2: number
}

interface IntermediateAsset {
    id: string,
    color: string,
    size: number,
    owner: string,
    appraisedValue: number,
    integerArrays: {
        randValues1: number[],
        randValues2: number[]
    }
}

interface ComplexAsset {
    orgMspId: string,
    empresa: {
        nombre: string,
        nombreComercial: string,
        email: string,
        telefono: number,
        direccion: string,
        poblacion: string,
        provincia: string,
        pais: string,
        cp: number,
        publicCert: string
        
        declaracionesConformidad: Array<{
            titulo: string,
            descripcion: string,
            estado: number,
            fechaIniVigencia: number,
            fechaFinVigencia: number,
            refDoc: string,
            hashDoc: string,
    
            leyes: Array<{
                titulo: string,
                descripcion: string,
                fechaIniVigencia: number,
                refDoc: string,
                hashDoc: string
            }>,
        }>,
    
        productos: Array<{
            codigo: string,
            nombre: string,
            descripcion: string
        }>
    },
}

type Asset = SimpleAsset | IntermediateAsset | ComplexAsset;
type AssetKV = { id: string, value: Asset };

function generateSimpleAsset(): SimpleAsset {
    function normalizeInteger(value: number): number {
        return Math.floor(Math.random() * value);
    }

    return {
        value1: normalizeInteger(201),
        value2: normalizeInteger(1001)
    };
}

function generateIntermediateAsset(id: string): IntermediateAsset {
    function generateRandIntArray(id: number, base: number, numValues: number, delta=1000): number[] {
        const randIntArray: number[] = [];
        for (let n = 0; n < numValues; n++) {
            randIntArray.push((base + n) * delta + id + id % numValues);
        }
        return randIntArray;
    }
    
    const assetGallery: Array<Partial<IntermediateAsset>> = [
        { color: 'blue', size: 5, owner: 'Tomoko', appraisedValue: 300 },
        { color: 'red', size: 5, owner: 'Brad', appraisedValue: 400 },
        { color: 'green', size: 10, owner: 'Jin Soo', appraisedValue: 500 },
        { color: 'yellow', size: 10, owner: 'Max', appraisedValue: 600 },
        { color: 'black', size: 15, owner: 'Adriana', appraisedValue: 700 },
        { color: 'white', size: 15, owner: 'Michel', appraisedValue: 800 }
    ];

    const assetIndex = parseInt(id.substring('id-intermediate-'.length));
    const assetTemplate = assetGallery[assetIndex % assetGallery.length];
    return {
        id,
        color: assetTemplate.color,
        size: assetTemplate.size,
        owner: assetTemplate.owner,
        appraisedValue: assetTemplate.appraisedValue,
        integerArrays: {
            randValues1: generateRandIntArray(assetIndex, 1, 2),
            randValues2: generateRandIntArray(assetIndex, 3, 6)
        }
    };
}

function generateComplexAsset(id?: string): ComplexAsset {
    function dateToTime(dateString: string): number {
        return new Date(dateString).getTime();
    }

    return {
        orgMspId: !!id ? id : "aimplas-notifierMSP",
        empresa: {
            nombre: "AIMPLAS", nombreComercial: "AIMPLAS - Instituto Tecnológico del Plástico", email: "aimplas@aimplas.es", telefono: 961366040, direccion: "Calle Gustave Eiffel, 4", poblacion: "Paterna", provincia: "València", pais: "España", cp: 46980,
            publicCert: "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAEAQCxC8nd59ynxsXRaUdMlMRDH+r4PZqinq0sSAMJ5Vg608393bUTkTEFAlH+crHoJe+Ec6vRnflYSruw55QLcKxVH73sAQiCboyW//9Xtsn404Sb8Ow7Dw5G4JHjG6LUrCED/mccp1IxuWlcIqnEtIu+7ORv2IS3bkeL6a8u/Vp7d4+zd6D1/1NycY/iwqf/Ab0K75oJZ8B29dGm9OqUYKEF8IIhc+tT047LbrFh2SVK3+UUbzEzxqoOII/pNakDUUgyP7exgsE1Ae1WY1M9WZhpSHB6eLgFDeyfBokZzFKrieys4w+IpYx/0fdvj4fD1jHs6xCTUjeDUsLYpC5Mf/KB8DtNQHC4wFcOd/PKK02Hx1X496hunLQa5H4mfO5/PifhS1NB5i1bMg+4uojGF9mCk06UJs1GdhLHgrL2AAbV8gZ1peB/3r9l1OTWVZ4ZfXE2eI6VM2WFfPq4r19qRyJiyLh55IRlKOGM5RwCn/5S/3ScsPVRXD7TUMosMDI2PgsG9HdxiNforEp9UfZQwRim6LSmTZxWp2ab/mV/IwSzgTJctb8HAioaulE+xk4FJi8qW/TjsVbPEnAw3dPthuk3Znc7mg79tDDKRc/KgVF2CVfvwyafUR8MaRMqgv6YA5ok2gv0KdrAKgjkrExAP2bTBVBg1E8CSTcvRd92rIBKX4XJAX1SBnp7ntWlC2J79ZFzJgS8YMaXiZw278xXuP3Qr2/goAyMP8/UYhoKz0iC18gJxBcdvR2f1PGOzUbFalPaN8p+xmCW1uCXQgULO2aJqEH7wyQWtetpkjPoxDnL0kkD0xujx6lfyaQ8ydDNrqQvMFHiWDLbppUqiAj03c86l0C+3qUT3a7WStSEmlYueRQ7gQFED3M/9SY9/6AYnmsQjPk6hRMeJkUAO2RAYbM/M5JRnldVhgbTVI6gDJ7kJ0XJsnIurycuO86csPMZuUc2DDJ9HWxW0k9M5flxrzUDeUTr1uSh6XllqETyC75/8CK7kT4uB73RoGU3IhmZ7sIt3uehlI4jqI1NAelw4hThLY8buuU/M6tpJqKD+yc7MHCoeKWpS8xdkjWyByJgCyH7vM5/VjfKhjhiEwOFDGUqaVp3evEoXTpx586kv/O4XnyK6icwZHRWyK+NJXqapoqeILPuKWjuNFWVlxU7sI4+WbClCQ1Q1zSBed+8YLqHJyCXdBVUPELj/oX5crETPTxdnxjc8Ef2Te75qQOR50rfAE3RvTleUpPAg+gVN2vWiwZ8aLFsOfCOI/w0eVNyJuba60Xcqh2DDzNcaz6AZahH9j6OaIhNbXPxJGJJo8AZfmU9FhfV+Y84uxxQ+X8ftntgscbTMZGoGXClnbgD5omH",
            declaracionesConformidad: [
                { titulo: "DEC-A-2019-305", descripcion: "ASDF", fechaIniVigencia: dateToTime("2019-12-17"), fechaFinVigencia: dateToTime("2019-12-19"), estado: 1, refDoc: "first-compounderMSP/DEC-A-2019-305.pdf'", hashDoc: "2YtfPIFz03zxbbsEkHcEroLgzf2LgzYTszKKstq0N0Ahq497vJ2N0nEGIONGIWebfJ6zaZWYqClwJu8Nnua0SPgAZiGwmw2BeJ25z6zz02BJF7BW6w3ndpPXp9QYATqC", leyes: [ { titulo: "Reglamento 10/2011", descripcion: "Cambios en la concentración máxima permitida de [...]", fechaIniVigencia: dateToTime("2011-10-07"), refDoc: "https://www.boe.es/doue/2011/012/L00001-00089.pdf", hashDoc: "ed461a9a5f1c7a861716c68dd8f9d3a095d6f7611a14f012570fe52f99fad6ba0017233d3182df0aefc5f8a2cb33f509a536124a26a93659862bef5968df7d23" } ] },
                { titulo: "DEC-B-2019-521", descripcion: "Modifica la normativa xxxx", fechaIniVigencia: dateToTime("2019-12-18"), fechaFinVigencia: null, estado: 2, refDoc: "second-compounderMSP/DEC-B-2019-521.pdf", hashDoc: "mAeGpW6VLFNGamG1YZ8q3tTiqVCOyGz2M6MHpO7quPrc2MPCWWrEkTmYVP6bYaojrviE1FrPbLh8mmnfu8YAVLsuz7iVajOxA7jq3yp8VnyNzruACXT2Yw9nBq4qXPjC", leyes: [ { titulo: "Reglamento 847/2011", descripcion: "Cambios en la concentración máxima permitida de [...]", fechaIniVigencia: dateToTime("2011-10-23"), refDoc: "https://eur-lex.europa.eu/legal-content/ES/TXT/PDF/?uri=CELEX:32011R0547&from=DE", hashDoc: "e88e2d3d58b5d21e8ba4dc62cd1914101d4877832038a2207ec5e699097e87cd6eaa5481cb280f96412407c5ae408b17ef137bceda3a12c394d35a6acd1b63cc" } ] },
                { titulo: "DEC-A-2020-060", descripcion: "Modifica la normativa 305", fechaIniVigencia: dateToTime("2019-12-19"), fechaFinVigencia: dateToTime("2019-12-21"), estado: 3, refDoc: "first-compounderMSP/DEC-A-2020-060.pdf", hashDoc: "L2ez7TLpBc1685H8X88Mpiu0HGqXrDaBrNKlvBRZsReEURu7z8RgJVKM1AnNk7MNd1IhTUZbscImdeCkfdRuVPxXd4wocJGbmytaS6qrvpkrSh72KurklWinLh4zQMTw", leyes: [ { titulo: "Reglamento CE 11/2012", descripcion: "...", fechaIniVigencia: dateToTime("2012-10-07"), refDoc: "https://www.boe.es/doue/2012/343/L00001-00029.pdf", hashDoc: "f917b14434b2fd599ccbfc190dba4d10c923988a3e94fcc09cc166e004dd94f6bb3814299eb57d2d151232eaf1af01f57872ebb12d82617c0707ff05c7d104f4" } ] },
                { titulo: "DEC-A-2020-365", descripcion: "x3x0x5x", fechaIniVigencia: dateToTime("2019-12-21"), fechaFinVigencia: null, estado: 4, refDoc: "first-compounderMSP/DEC-A-2020-365.pdf", hashDoc: "vuPMG7EYNCId7Yp6CstkVWUvas3ML5eBHIyKXnEhUOnhTLE7OcgHBBvbrZMP22nvN9p8g3w4D4sW28pk4FirNMxc00eUYW38FxjPJrbcUULi1w8KwJKP7mlIZVetrzev", leyes: [ { titulo: "Real Decreto 874/2012", descripcion: "...", fechaIniVigencia: dateToTime("2012-10-23"), refDoc: "https://www.boe.es/doue/2012/258/L00001-00020.pdf", hashDoc: "4656f40f3b2f70fb56213beb1d591ce35b1b390338fafdef776ce7a18a7f09f4e66d815b8c7f299595dbc4cc680882a8d22da07000bd5ad1c78887d45b810cbb" } ] }
            ],
            productos: [ { codigo: "6020600-A", nombre: "Producto A", descripcion: "Producto 6020600-A" }, { codigo: "6060200-B", nombre: "Producto B", descripcion: "Producto 6060200-B" } ]
        }
    };
}

function generateAsset(assetType: AssetType, id?: string): Asset {
    switch (assetType) {
        case AssetType.Simple:
            return generateSimpleAsset();
        case AssetType.Intermediate:
            return generateIntermediateAsset(id);
        case AssetType.Complex:
            return generateComplexAsset(id);
    }
}

function* assetKeyValueGenerator(assetType: AssetType, numAssets?: number):
        Generator<AssetKV> {
    const maxAssets = 70000;
    numAssets = Math.max(numAssets, maxAssets) || maxAssets;
    switch (assetType) {
        case AssetType.Simple:
            for (let n = 1; n <= numAssets; n++) {
                const id = `id-simple-${n}`;
                yield { id, value: generateSimpleAsset() };
            }
            break;
        case AssetType.Intermediate:
            for (let n = 1; n <= numAssets; n++) {
                const id = `id-intermediate-${n}`;
                yield { id, value: generateIntermediateAsset(id) };
            }
            break;
        case AssetType.Complex:
            for (let n = 1; n <= numAssets; n++) {
                const id = `id-complex-${n}`;
                yield { id, value: generateComplexAsset(id) };
            }
    }
}

export { Asset,AssetKV, AssetType, SimpleAsset, IntermediateAsset, ComplexAsset,
    generateSimpleAsset, generateIntermediateAsset, generateComplexAsset,
    generateAsset, assetKeyValueGenerator };
