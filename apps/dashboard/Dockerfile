FROM node:20-alpine

WORKDIR /app

RUN echo '{"name":"aim-dashboard","version":"0.1.0","scripts":{"start":"node server.js"}}' > package.json

COPY . .

EXPOSE 3000

CMD ["node", "-e", "const h=require('http');h.createServer((req,res)=>{res.writeHead(200);res.end('AIM Dashboard OK')}).listen(3000,()=>console.log('Dashboard ready ✅'))"]