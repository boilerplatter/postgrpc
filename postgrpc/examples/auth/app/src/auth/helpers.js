// map Kratos flow nodes into form field components
export const toFields = flow => flow.nodes?.map(({ group, attributes: { value, ...attributes }, meta }) => {
  const isHidden = attributes?.type === 'hidden'
  const isButton = attributes?.type === 'submit'
  const hasLabel = !(isHidden || isButton) && meta?.label
  const key = meta?.label ? meta.label.text : attributes.group

  if (isButton) {
    return <>
      <input hidden {...attributes} value={value} />
      <button type='submit'>
        {meta.label.text}
      </button>
    </>
  }

  return <div key={key}>
    {
      hasLabel
        ? <label htmlFor={attributes.name || group}>{meta.label.text}:</label>
        : null
    }
    <input {...attributes} defaultValue={value} />
  </div>
})

