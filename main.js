import fs from 'fs'
import readline from 'readline'

const maxSize = 1024 * 1024 * 500 //~

async function writeLinesToFile(lines, index){
    return new Promise((resolve, reject) => {
        lines.sort()
        const writeStream = fs.createWriteStream((`./out/${index}.txt`))

        lines.forEach((line, index) => {
            writeStream.write(`${line}${index === lines.length - 1 ? '' : '\n'}`)
        })

        writeStream.end(() => {
            resolve()
        })
        writeStream.on('error', err => {
            console.log(`Ошибка записи в файл ${index}.txt`, err)
            reject(err)
        })
    })
}

async function splitFile(){
    return new Promise((resolve, reject) => {

        let arrOfLines = []
        let countOfFiles = 1
        let currentSize = 0

        if (!fs.existsSync('./out')) {
            fs.mkdirSync('./out')
        }

        const readStream = fs.createReadStream('./in.txt')
        const readLine = readline.createInterface({
            input : readStream,
            crlfDelay: Infinity
        })

        readLine.on('line', line => {
            currentSize += line.length * 4 // из соображений, что символ в строке <= 4 байт

            if(currentSize < maxSize)
                arrOfLines.push(line)
            else {
                writeLinesToFile(arrOfLines, countOfFiles)

                arrOfLines.length = 0
                arrOfLines.push(line)
                currentSize = line.length * 4
                countOfFiles++
            }
        })

        readLine.on('close', async() => {
            if(arrOfLines.length > 0){
                await writeLinesToFile(arrOfLines, countOfFiles)
            }
            resolve()
        })

        readLine.on('error', err => {
            console.log(`Ошибка при чтении файла in.txt`, err)
            reject(err)
        })
    })
}

async function getFinalFile(){
    const allFiles = fs.readdirSync('./out').map(file => file = './out/' + file)

    fs.openSync('./finalFile.txt', 'w')

    const writeStream = fs.createWriteStream('./finalFile.txt')
    const readStreams = allFiles.map(file => fs.createReadStream(file))
    const readlines = readStreams.map(stream => readline.createInterface({ 
        input: stream, 
        crlfDelay: Infinity 
    }))

    const objOfLines = allFiles.map(() => ({ value: '', done : false}))

    await Promise.all(
        readlines.map(async(stream, index) => {
            objOfLines[index] = await stream[Symbol.asyncIterator]().next()
        })
    )

    const iterators = readlines.map(r => r[Symbol.asyncIterator]())

    while(objOfLines.some(line => line.done == false)){
        let minLine = null
        let minIndex = -1

        objOfLines.forEach((line, index) => {
            if(line.done == false && (minLine === null || line.value < minLine)){
                minLine = line.value
                minIndex = index
            }
        })

        writeStream.write(`${minLine}\n`)
        objOfLines[minIndex] = await iterators[minIndex].next()
    }
    
    writeStream.end(() => {
        readStreams.forEach(stream => stream.close())
        fs.rm('./out', {recursive: true,}, err =>{
            if(err) console.log(err)
        })
    })
    
    writeStream.on('error', err => {
        console.error('Ошибка при записи в финальный файл:', err)
        readStreams.forEach(stream => stream.close())
    })
}

async function main(){
    await splitFile()
    getFinalFile()
}

main()
