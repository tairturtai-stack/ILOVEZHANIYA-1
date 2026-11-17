import express from 'express';import http from 'http';import {Server} from 'socket.io';
import {randomInt,randomUUID} from 'crypto';import fs from 'fs';import os from 'os';import net from 'net';import path from 'path';import {fileURLToPath} from 'url';
const app=express(),srv=http.createServer(app),io=new Server(srv);
const dir=path.dirname(fileURLToPath(import.meta.url));
const upDir=path.join(os.tmpdir(),'ilz-uploads');fs.rmSync(upDir,{recursive:true,force:true});fs.mkdirSync(upDir,{recursive:true});
const MAX=(+process.env.MAX_UPLOAD_MB||1024)*1048576;
function dropFile(r){if(r.file){fs.rm(path.join(upDir,r.file),{force:true},()=>{});r.file=null}}
function dropRoom(r){dropFile(r);rooms.delete(r.code)}
// загрузка собственного видео (MP4/WebM): только администратор комнаты
app.post('/upload',(req,res)=>{
 const r=rooms.get(String(req.query.code||'').toUpperCase());
 if(!r||r.admin!==String(req.query.pid||'')||!r.members.has(r.admin))return res.status(403).json({ok:false,error:'Нет прав на загрузку'});
 const len=+req.headers['content-length']||0;
 if(!len||len>MAX)return res.status(413).json({ok:false,error:'Файл больше '+Math.round(MAX/1048576)+' МБ'});
 if(r.uploading)return res.status(409).json({ok:false,error:'Загрузка уже идёт'});
 let id=null,ws=null,size=0,done=false;r.uploading=true;
 const fail=(c,e)=>{if(done)return;done=true;r.uploading=false;req.unpipe();if(ws)ws.destroy();if(id)fs.rm(path.join(upDir,id),{force:true},()=>{});res.status(c).json({ok:false,error:e})};
 req.on('data',ch=>{if(done)return;
  if(!ws){let ext=null;
   if(ch.length>=12&&ch.subarray(4,8).toString()==='ftyp')ext='mp4';
   else if(ch.length>=4&&ch[0]===0x1a&&ch[1]===0x45&&ch[2]===0xdf&&ch[3]===0xa3)ext='webm';
   else return fail(415,'Нужен файл MP4 или WebM');
   id=randomUUID()+'.'+ext;ws=fs.createWriteStream(path.join(upDir,id));ws.on('drain',()=>req.resume());ws.on('error',()=>fail(500,'Ошибка записи на диск'))}
  size+=ch.length;if(size>MAX)return fail(413,'Файл слишком большой');
  if(!ws.write(ch))req.pause()});
 req.on('end',()=>{if(done)return;if(!ws)return fail(400,'Пустой файл');
  ws.end(()=>{if(done)return;done=true;r.uploading=false;dropFile(r);r.file=id;r.last=Date.now();
   r.state={url:'/media/'+id,playing:false,pos:0,rate:1,ts:Date.now()};io.to(r.code).emit('state',r.state);res.json({ok:true})})});
 req.on('error',()=>fail(400,'Загрузка прервана'));req.on('aborted',()=>fail(400,'Загрузка прервана'))});
app.get('/media/:f',(req,res)=>{if(!/^[0-9a-f-]{36}\.(mp4|webm)$/.test(req.params.f))return res.sendStatus(404);
 res.sendFile(path.join(upDir,req.params.f),e=>{if(e&&!res.headersSent)res.sendStatus(404)})});
app.get('/',(_q,r)=>r.sendFile(path.join(dir,'watch.html')));
const rooms=new Map(),TTL=30*60*1000,ALPHA='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const clean=(s,n)=>String(s??'').replace(/[<>]/g,'').trim().slice(0,n);
const privIp=/^(10\.|127\.|192\.168\.|169\.254\.|172\.(1[6-9]|2\d|3[01])\.|0\.|::1$|f[cd])/i;
function safeUrl(u){try{const x=new URL(String(u));if(!['http:','https:'].includes(x.protocol))return null;
const h=x.hostname;if(h==='localhost'||h.endsWith('.local')||(net.isIP(h)&&privIp.test(h)))return null;return x.href}catch{return null}}
const members=r=>[...r.members.entries()].map(([pid,m])=>({name:m.name,online:m.online,admin:pid===r.admin}));
const push=r=>io.to(r.code).emit('room',members(r));
io.on('connection',s=>{
let hits=[];
const on=(ev,fn)=>s.on(ev,(d,cb)=>{const n=Date.now();hits=hits.filter(t=>n-t<10000);if(hits.length>=40)return;hits.push(n);
try{fn(d||{},typeof cb==='function'?cb:()=>{})}catch(e){console.error(e)}});
const cur=()=>{const r=rooms.get(s.data.code);return r&&r.members.has(s.data.pid)?r:null};
const admin=()=>{const r=cur();return r&&r.admin===s.data.pid?r:null};
on('time',(_d,cb)=>cb(Date.now()));
on('create',(d,cb)=>{const pid=clean(d.pid,64);if(!pid)return cb({ok:false});
let code;do{code=Array.from({length:6},()=>ALPHA[randomInt(ALPHA.length)]).join('')}while(rooms.has(code));
rooms.set(code,{code,admin:pid,members:new Map(),last:Date.now(),state:{url:'',playing:false,pos:0,rate:1,ts:Date.now()},msgs:[]});cb({ok:true,code})});
on('join',(d,cb)=>{const code=clean(d.code,8).toUpperCase(),pid=clean(d.pid,64),r=rooms.get(code);
if(!r)return cb({ok:false,error:'Комната не найдена или закрыта'});
if(!r.members.has(pid)){if(r.members.size>=2)return cb({ok:false,error:'В комнате уже два участника'});
if(!r.members.size&&pid!==r.admin)return cb({ok:false,error:'Комната ещё не готова'})}
const m=r.members.get(pid)||{};m.name=clean(d.name,24)||'Гость';m.online=true;m.sid=s.id;r.members.set(pid,m);
s.data={pid,code};s.join(code);push(r);cb({ok:true,admin:r.admin===pid,members:members(r),state:r.state,msgs:r.msgs,now:Date.now()})});
on('source',(d,cb)=>{const r=admin();if(!r)return;const url=safeUrl(d.url);
if(!url)return cb({ok:false,error:'Допустимы только публичные ссылки http(s)'});
dropFile(r);r.state={url,playing:false,pos:0,rate:1,ts:Date.now()};io.to(r.code).emit('state',r.state);cb({ok:true})});
on('ctl',d=>{const r=admin();if(!r)return;const st=r.state,now=Date.now();
const pos=Number(d.pos),rate=Number(d.rate);
if(Number.isFinite(pos)&&pos>=0)st.pos=pos;
if(d.type==='play')st.playing=true;else if(d.type==='pause')st.playing=false;
if(d.type==='rate'&&rate>=0.5&&rate<=2)st.rate=rate;
st.ts=now;r.last=now;io.to(r.code).emit('state',st)});
on('chat',d=>{const r=cur();if(!r)return;const text=clean(d.text,500);if(!text)return;
const m={name:r.members.get(s.data.pid).name,text,t:Date.now()};r.msgs.push(m);r.msgs=r.msgs.slice(-100);io.to(r.code).emit('chat',m)});
on('leave',()=>{const r=cur();if(!r)return;s.leave(r.code);if(r.admin===s.data.pid)r.members.get(s.data.pid).online=false;else r.members.delete(s.data.pid);s.data={};push(r)});
on('close',()=>{const r=admin();if(!r)return;io.to(r.code).emit('closed');dropRoom(r)});
on('buffer',d=>{const r=cur();if(!r)return;for(const [p,m] of r.members)if(p!==s.data.pid&&m.online)io.to(m.sid).emit('buffer',{b:!!d.b})});
on('signal',d=>{const r=cur();if(!r||JSON.stringify(d).length>8000)return;
for(const [p,m] of r.members)if(p!==s.data.pid&&m.online)io.to(m.sid).emit('signal',d)});
s.on('disconnect',()=>{const r=cur();if(!r)return;const m=r.members.get(s.data.pid);if(m.sid===s.id){m.online=false;r.last=Date.now();push(r)}});
});
setInterval(()=>{for(const [c,r] of rooms)if(![...r.members.values()].some(m=>m.online)&&Date.now()-r.last>TTL)dropRoom(r)},60000);
app.use((_q,r)=>r.sendFile(path.join(dir,'watch.html')));
srv.listen(process.env.PORT||3000,()=>console.log('ILOVEZHANIYA на порту',process.env.PORT||3000));
