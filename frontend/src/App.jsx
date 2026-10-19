import { useState } from 'react'
import search from './assets/search.svg'
import './App.css'


const backend = import.meta.env.VITE_BACKEND_URL
function App() {
	const [results, setResults] = useState([])
	const [query, setQuery] = useState("")
	const [loading, setLoading] = useState(false)

	async function fetchResults(){

		setLoading(true)
		let result = await fetch(`${backend}/search?q=${query}`)
		let final = await result.json()
		setResults(final)
		setLoading(false)
	}


  return (
  <div className='flex flex-col items-center w-screen bg-slate-900'>
	<div className='h-screen w-full  xl:w-1/2 bg-slate-950 flex-col flex px-8 '>
		<div className=' w-full bg-gray-200 rounded-xl m-2  shrink-0 overflow-hidden flex items-center text-black'>
			<img src={search} className='w-6 h-6 shrink-0 mx-2' alt='search' />
			<input className=' p-2  w-full  ' placeholder="search on kamenx" onChange={e=>{
			setQuery(e.target.value)
			}} onKeyDown={e=>{
			if (e.key === "Enter"){
				fetchResults()	
			}
			}}/>
		</div>	
		{!loading ? <div className='flex flex-col'>
			{results.map((e,i)=>{
			return(
				<div className='bg-slate-800 text-white rounded-xl my-2  w-full p-4 group cursor-pointer mx-2' key={i} onClick={()=>{
					location.href = e.url
				}}>
					<p className='text-xl group-hover:underline cursor-pointer'>{e.url}</p>
					<p className='text-gray-300 ml-1'>{e.snippet.slice(0,100)}</p>
				</div>
				)
			})}
			
		</div> : <div className='flex items-center justify-center h-1/2'><div className='w-max aspect-square rounded-md p-2 text-white text-xl items-center justify-center flex animate-spin bg-slate-800'>Loading</div></div>}
    </div>
	</div>
  )
}

export default App
